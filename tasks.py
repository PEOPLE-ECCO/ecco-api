import logging
import os
import tempfile

from celery import shared_task
from celery.signals import worker_ready
from minio import Minio
from quart import current_app
from sqlalchemy import select, or_

from eo.api import OpenEO, OpenEOProcess
from storage.db import DBSession
from storage.definitions import *

LOGGER = logging.getLogger(__name__)
LOGGER.setLevel(logging.DEBUG)


@worker_ready.connect
def at_start(sender, **k):
    init.apply_async([], countdown=10)


@shared_task()
def init():
    with DBSession(admin=True) as sess:
        unfinished_jobs = [a.id for a in sess.scalars(select(Job).where(or_(Job.status == JobStatus.CREATED, Job.status == JobStatus.QUEUED, Job.status == JobStatus.RUNNING)))]
        for j in unfinished_jobs:
            job_status.delay(j)
            LOGGER.info(f"Scheduling job_status for unfinished job: {j}")


@shared_task()
def schedule_job(job_id: str):
    openeo: OpenEO = current_app.config["openeo"]
    with DBSession(admin=True) as sess:
        job = sess.get(Job, job_id)

        eoprocess = OpenEOProcess(
            None,
            None
        )

        job.openeo_id = openeo.schedule_process(eoprocess)
        job.status = JobStatus.QUEUED

        job_status.apply_async([job_id], countdown=10)
        sess.commit()


@shared_task()
def job_status(job_id: str):
    openeo: OpenEO = current_app.config["openeo"]

    LOGGER.info(f"fetching status for job {job_id}")
    with DBSession(admin=True) as sess:
        job = sess.get(Job, job_id)
        current_state = openeo.get_status(job.openeo_id)

        finished = False
        match current_state:
            case "created":
                state = JobStatus.CREATED
            case "queued":
                state = JobStatus.QUEUED
            case "running":
                state = JobStatus.RUNNING
                job_get_log.delay(job_id)
            case "canceled":
                state = JobStatus.CANCELED
                job_get_log.delay(job_id)
                finished = True
            case "finished":
                state = JobStatus.AWAITING_RESULT_DOWNLOAD
                job_get_log.delay(job_id)
                job_download_results.delay(job_id)
                finished = True
            case "error":
                state = JobStatus.ERROR
                job_get_log.delay(job_id)
                finished = True

        if state != job.status:
            LOGGER.info(f"Status changed! was: {job.status} now: {current_state}")
            job.status = state
            sess.commit()

        if not finished:
            # Reschedule if not finished yet
            job_status.apply_async([job_id], countdown=60)


@shared_task()
def job_get_log(job_id: str):
    openeo: OpenEO = current_app.config["openeo"]
    LOGGER.info(f"getting log for job {job_id}")
    with DBSession(admin=True) as sess:
        job = sess.get(Job, job_id)
        job.log = openeo.get_log(job.openeo_id).logs
        sess.commit()


@shared_task()
def job_download_results(job_id: str):
    openeo: OpenEO = current_app.config["openeo"]
    LOGGER.info(f"downloading results for job {job_id}")
    with DBSession(admin=True) as sess:
        job = sess.get(Job, job_id)
        ts = sess.get(Timeseries, job.timeseries_id)

        s3: Minio = current_app.config["S3"]
        with tempfile.TemporaryDirectory(delete=True) as tmpdirname:
            openeo.download_results(job.openeo_id, tmpdirname)
            for result in os.listdir(tmpdirname):
                f = open(os.path.join(tmpdirname, result), "rb")
                # Find out file size
                f.seek(0, os.SEEK_END)
                size = f.tell()
                f.seek(0)

                LOGGER.info(f"uploading {result} to s3:{ts.bucket}/{job_id}/")
                s3.put_object(ts.bucket, f"{job_id}/{result}", f, size)
                LOGGER.info(f"done uploading {result} to s3:{ts.bucket} ")

        job.state = JobStatus.COMPLETED
        sess.commit()
