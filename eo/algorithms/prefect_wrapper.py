import importlib
import json
import os
from abc import ABC
from io import BytesIO
from typing import Dict

import openeo
import pystac
from minio import Minio
from openeo.rest.connection import Connection
from prefect import flow, task, get_client
from prefect.blocks.system import Secret
from prefect.cache_policies import NO_CACHE
from prefect.client.orchestration import SyncPrefectClient
from prefect.runtime import flow_run
from pystac import Catalog, ItemCollection


class RunnableAlgorithm(ABC):

    @staticmethod
    def run(conn: Connection, catalog: Catalog, parameters: Dict):
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
        print(f"Tracking execution for job: {job.job_id}")
        jobs.append(job)
        return job

    conn.execute = None  # We prevent sync execution until we have it logged
    conn.create_job_orig = conn.create_job
    conn.create_job = create_job_logged  # log all batch jobs to enable tracking

    return conn


@task(log_prints=True, cache_policy=NO_CACHE)
def persist_outputs(catalog: Catalog) -> None:
    s3_url = Secret.load("ecco-s3-url").get()
    s3_akey = Secret.load("ecco-s3-access-key").get()
    s3_skey = Secret.load("ecco-s3-secret-key").get()
    client = Minio(
        s3_url,
        secure=True,
        access_key=s3_akey,
        secret_key=s3_skey,
        region="garage"
    )
    client.make_bucket(flow_run.name)

    # bboxes = [item.bbox for item in catalog.get_items(recursive=True)]
    # min_x = min(b[0] for b in bboxes)
    # min_y = min(b[1] for b in bboxes)
    # max_x = max(b[2] for b in bboxes)
    # max_y = max(b[3] for b in bboxes)
    # new_spatial = SpatialExtent([[min_x, min_y, max_x, max_y]])
    #
    # collection = Collection(
    #     id=flow_run.name,
    #     description=f"Collection for Flow: {flow_run.name}",
    #     extent=Extent(new_spatial, TemporalExtent([[datetime.now(tz=timezone.utc), None]])),
    #     catalog_type=pystac.CatalogType.ABSOLUTE_PUBLISHED,
    #     extra_fields={
    #         "flow_run.name": flow_run.name,
    #     }
    # )
    meta = {
        "flow_run.name": flow_run.name
    }
    collection = ItemCollection(items=catalog.get_items(recursive=True), extra_fields=meta)

    for item in catalog.get_items(recursive=True):
        for asset in item.get_assets().values():
            upload_file_to_s3(client, flow_run.name, asset.href)

    store_file(client, "stac", collection.to_dict(), "collection")


@task(log_prints=True, cache_policy=NO_CACHE)
def store_file(client: Minio, filename: str, content, filetype: str = "json") -> None:
    io = BytesIO()
    dump = json.dumps(content)
    io.write(dump.encode())
    io.seek(0)
    client.put_object(flow_run.name, f"{filename}.{filetype}", io, io.getbuffer().nbytes)


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
def persist_logs() -> None:
    costs = 0
    for job in jobs:
        try:
            log = job.logs()
            description = job.describe()

            print(log)  # also store in prefect logs
            print(description)  # also store in prefect logs

            if "costs" in description:
                costs += description["costs"]
        except Exception as e:
            print(f"could not persist logs! {e}")

    client: SyncPrefectClient = get_client(sync_client=True)
    tags = flow_run.get_tags()
    tags.append(f"costs:{costs}")
    client.update_flow_run(flow_run.id, tags=list(tags))


@task(log_prints=True, cache_policy=NO_CACHE)
def run(algo: RunnableAlgorithm, conn: Connection, catalog: Catalog, parameters: Dict) -> None:
    algo.run(conn, catalog, parameters)


@flow(log_prints=True)
def run_algorithm(parameters: Dict) -> None:
    algo = getattr(importlib.import_module(os.getenv("ALGORITHM_BASE")), "Algorithm")
    conn = get_openeo_connection()
    try:
        catalog = pystac.Catalog(id=flow_run.name, description=f"Catalog for Flow: {flow_run.name}")
        run(algo, conn, catalog, parameters)
        persist_outputs(catalog)
    except Exception as e:
        print(e)
    finally:
        persist_logs()
