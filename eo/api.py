import logging
from dataclasses import dataclass
from datetime import datetime
from typing import Optional, Iterable, Dict

import openeo
from openeo.rest.connection import Connection
from openeo.rest.models.general import LogsResponse
from openeo.udf.run_code import extract_udf_dependencies
from storage.definitions import Process

LOGGER = logging.getLogger(__name__)
LOGGER.setLevel(logging.DEBUG)

logging.getLogger("openeo.rest.connection").setLevel(logging.DEBUG)


@dataclass(frozen=True)
class OpenEOConfig:
    backend: str
    oidc_provider: str


class SpatialExtent(Dict):
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
    max_cloud_cover: Optional[float] = None


@dataclass(frozen=True)
class OpenEOProcess:
    process: Process
    collection: Collection


class OpenEO:
    config: OpenEOConfig
    connection: Connection | None
    initialized = False

    def __init__(self, config: OpenEOConfig):
        self.config = config

    def _init(self):
        self.connection = openeo.connect(self.config.backend).authenticate_oidc_device(provider_id=self.config.oidc_provider, store_refresh_token=True)
        LOGGER.debug(self.connection.list_udf_runtimes()["Python"]["versions"]["3"]["libraries"])
        self.initialized = True

    def schedule_process(self, to_run: OpenEOProcess) -> str:
        """
            Creates job-datacube and schedules it as a batch job in the openeo backend
        """
        if not self.initialized:
            self._init()
        for _ in range(5):
            LOGGER.debug("OVERWRITING PROCESS WITH HARDCODED DEBUGGING PROCESS")
        to_run = OpenEOProcess(
            None,
            Collection(
                "SENTINEL2_L2A",
                spatial_extent={"west": 4.00, "south": 51.04, "east": 4.10, "north": 51.1},
                temporal_extent=["2022-03-01", "2022-03-31"],
                bands=["B02", "B03", "B04"]
            )
        )

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
        udf = openeo.UDF.from_file(script_wrapper)
        LOGGER.debug("Installing the following dependencies:")
        LOGGER.debug(extract_udf_dependencies(udf))

        cube = cube.apply(udf)

        eojob = cube.create_job()
        eojob.start()
        print(eojob)
        print(eojob.job_id)
        return eojob.job_id

    def get_status(self, openeo_id: str) -> str:
        """
        Gets the current status of a Job
        """
        if not self.initialized:
            self._init()
        job = self.connection.job(openeo_id)
        return job.status()

    def get_log(self, openeo_id: str, level: int = logging.INFO) -> LogsResponse:
        """
        Gets the log of a Job
        """
        if not self.initialized:
            self._init()
        job = self.connection.job(openeo_id)
        return job.logs(level=level)

    def download_results(self, openeo_id: str, output_dir: str) -> None:
        """
        Downloads the results of a Job
        """
        if not self.initialized:
            self._init()
        job = self.connection.job(openeo_id)
        results = job.get_results()
        results.download_files(output_dir)
