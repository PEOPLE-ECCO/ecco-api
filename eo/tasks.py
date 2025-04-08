from pathlib import Path

from celery import shared_task
from openeo.udf import execute_local_udf
from quart import current_app

from api import OpenEO


@shared_task()
def test():
    openeo: OpenEO = current_app.config["openeo"]
    print(openeo)

    s2_cube = openeo.connection.load_collection(
        "SENTINEL2_L2A",
        spatial_extent={"west": 4.00, "south": 51.04, "east": 4.10, "north": 51.1},
        temporal_extent=["2022-03-01", "2022-03-31"],
        bands=["B02", "B03", "B04"]
    )

    s2_cube.download('test_input.nc', format='NetCDF')
    local_udf = Path('script.py').read_text()
    result_cube = execute_local_udf(local_udf, 'test_input.nc', fmt='netcdf')
    print(result_cube)

    #    job = rescaled.create_job()
    #    job.start_and_wait()
    #    job.download_files(Path("./results"))


@shared_task()
def download_job_results(job: Job):
    openeo: OpenEO = current_app.config["openeo"]

    openeo.download_results(job_id=)
