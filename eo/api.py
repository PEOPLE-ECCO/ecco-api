import logging
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Optional, Iterable

import openeo
from openeo.udf.run_code import extract_udf_dependencies
from storage.definitions import Process

LOGGER = logging.getLogger(__name__)
LOGGER.setLevel(logging.INFO)


@dataclass(frozen=True)
class OpenEOConfig:
    backend: str


class SpatialExtent:
    west: float
    south: float
    east: float
    north: float


@dataclass(frozen=True)
class Collection:
    name: str
    spatial_extent: SpatialExtent
    temporal_extent: (datetime.date, datetime.date)
    bands: Iterable[str]
    max_cloud_cover: Optional[float]


@dataclass(frozen=True)
class OpenEOProcess:
    process: Process
    collection: Collection
    collection: Collection


class OpenEO:

    def __init__(self, config: OpenEOConfig):
        self.connection = openeo.connect(config.backend).authenticate_oidc_device(store_refresh_token=True)
        LOGGER.debug(self.connection.list_udf_runtimes()["Python"]["versions"]["3"]["libraries"])
        pass

    def schedule_process(self, to_run: OpenEOProcess) -> str:
        """
            Creates job-datacube and schedules it as a batch job in the openeo backend
        """

        cube = self.connection.load_collection(
            to_run.collection.name,
            spatial_extent=to_run.collection.spatial_extent,
            temporal_extent=to_run.collection.temporal_extent,
            bands=to_run.collection.bands,
            max_cloud_cover=to_run.collection.max_cloud_cover
        )

        # TODO: generate generic script that imports the actual process-repository as a library
        #  we can only give a single python file here.
        script_wrapper = "./script.py"
        udf = openeo.UDF.from_url(script_wrapper)
        LOGGER.debug("Installing the following dependencies:")
        LOGGER.debug(extract_udf_dependencies(udf))

        cube.apply(udf)

        job = cube.create_job()
        job.start()
        return job.job_id

    def get_status(self, job_id: str) -> str:
        """
        Gets the current status of a Job
        """

        job = self.connection.job(job_id)
        return job.status()

    def get_log(self, job_id: str) -> str:
        """
        Gets the log of a Job
        """

        job = self.connection.job(job_id)
        return job.logs(level=logging.INFO)

    def download_results(self, job_id: str, output_dir: Path) -> None:
        """
        Downloads the results of a Job
        """

        job = self.connection.job(job_id)
        results = job.get_results()

        results.download_files()
