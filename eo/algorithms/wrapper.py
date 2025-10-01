import glob
import importlib
import os
import tempfile
from abc import ABC, abstractmethod
from typing import Dict

import openeo
from minio import Minio
from openeo.rest.connection import Connection
from prefect import flow, task
from prefect.blocks.system import Secret
from prefect.cache_policies import NO_CACHE
from prefect.runtime import flow_run


class RunnableAlgorithm(ABC):

    @abstractmethod
    def run(self, conn: Connection, output_dir: str, parameters: Dict):
        pass


jobs = []


@task(log_prints=True, cache_policy=NO_CACHE)
def get_openeo_connection() -> Connection:
    cdse_client_id = Secret.load("ecco-openeo-client-id").get()
    cdse_client_secret = Secret.load("ecco-openeo-client-secret").get()

    conn = openeo.connect("https://openeofed.dataspace.copernicus.eu/").authenticate_oidc_client_credentials(
        client_id=cdse_client_id,
        client_secret=cdse_client_secret,
        provider_id="CDSE"
    )

    def create_job_logged(*args, **kwargs):
        job = conn.create_job_orig(*args, **kwargs)
        jobs.append(job)
        return job

    conn.execute = None  # We prevent sync execution until we have it logged
    conn.create_job_orig = conn.create_job
    conn.create_job = create_job_logged # log all batch jobs to enable tracking

    return conn


@flow()
def persist_outputs(conn: Connection, output_dir: str) -> None:
    s3_url = Secret.load("ecco-s3-url").get()
    s3_akey = Secret.load("ecco-s3-access-key").get()
    s3_skey = Secret.load("ecco-s3-secret-key").get()
    client = Minio(
        s3_url,
        secure=True,
        access_key=s3_akey,
        secret_key=s3_skey
    )
    client.make_bucket(flow_run.name)

    for filename in glob.iglob(output_dir + '**/**', recursive=True):
        if os.path.isdir(filename):
            continue
        upload_file_to_s3(client, flow_run.name, filename)

    persist_logs(conn)


@task(log_prints=True, cache_policy=NO_CACHE)
def upload_file_to_s3(client: Minio, bucket: str, name: str) -> None:
    print(f"Start uploading file to s3: {name}")
    with open(name, 'rb') as file:
        # Find out file size
        file.seek(0, os.SEEK_END)
        size = file.tell()
        file.seek(0)
        client.put_object(bucket, name, file, size)
    print(f"Done uploading file to s3: {name}")


@task(log_prints=True, cache_policy=NO_CACHE)
def persist_logs(conn: Connection) -> None:
    for job in jobs:
        try:
            log = job.logs()
            print(log)
            description = job.describe()
            print(description)
        except Exception as e:
            print(f"could not persist logs! {e}")


@task(log_prints=True, cache_policy=NO_CACHE)
def run(algo: RunnableAlgorithm, conn: Connection, output_dir: str, parameters: Dict) -> None:
    algo.run(conn, output_dir, parameters)


@flow(log_prints=True)
def run_algorithm(parameters: Dict) -> None:
    algo = getattr(importlib.import_module(os.getenv("ALGORITHM_BASE")), "Algorithm")
    conn = get_openeo_connection()
    with tempfile.TemporaryDirectory() as output_dir:
        run(algo, conn, output_dir, parameters)
        persist_outputs(conn, output_dir)
