import json
import os
import pathlib
import sys
from typing import Dict
from uuid import UUID

import click
import openeo
from dotenv import load_dotenv
from prefect import deploy as deploy_to_prefect
from prefect.blocks.system import Secret
from prefect.docker import DockerImage
from sqlalchemy import Engine
from sqlalchemy.orm import Session

from storage.db import DBSession, recreate_tables, DBConfig, connect_db
from storage.definitions import Process, Scenario, Timeseries

load_dotenv()


@click.group()
def cli():
    pass


engine: Engine = connect_db(DBConfig(
    host=os.environ.get("ECCO_DB_HOST", "localhost"),
    port=int(os.environ.get("ECCO_DB_PORT", 5431)),
    user=os.environ.get("POSTGRES_ROOT_USER", ""),
    password=os.environ.get("POSTGRES_ROOT_PASSWORD", ""),
    dbname=os.environ.get("ECCO_DB_DBNAME", "ecco")
))


# s3_url = Secret.load("ecco-s3-url").get()
# s3_akey = Secret.load("ecco-s3-access-key").get()
# s3_skey = Secret.load("ecco-s3-secret-key").get()
#
# s3: Minio = Minio(
#     s3_url,
#     secure=True,
#     access_key=s3_akey,
#     secret_key=s3_skey,
#     region="garage"
# )


@click.command()
def recreate_db():
    click.echo('Initializing the database')
    recreate_tables(engine)


@click.command()
def adddata():
    with DBSession(admin=True, engine=engine) as sess:
        uuids = {
            "n52_test": "e9227aaf-0dc9-4fae-9181-28ed61dca883"
        }
        add_example_scenarios(sess, uuids)
        add_example_processes(sess, uuids)


def add_example_scenarios(sess: Session, uuids: Dict[str, str]):
    example_scenario_2 = Scenario(
        name="Garamba National Park",
        bbox="28.430594125701077 3.0484258547741234, 30.28153276793921 4.892387834055029",
        description="Case Study at the GNP",
        preview_image="https://images.unsplash.com/photo-1557050543-4d5f4e07ef46?q=80&w=2664&auto=format&fit=crop&ixlib=rb-4.0.3&ixid=M3wxMjA3fDB8MHxwaG90by1wYWdlfHx8fGVufDB8fHx8fA%3D%3D",
        acl_read=uuids.values()
    )

    example_scenario_3 = Scenario(
        name="Lebanon",
        bbox="36.11770825094604 33.95054257445673, 36.221846157438165 34.0379427366216",
        description="Case Study in Libanon",
        preview_image="/static/lebanon.png",
        acl_read=uuids.values()
    )

    example_scenario_4 = Scenario(
        name="Sông Mã (Vietnam)",
        bbox="103.99957884606624 21.04326409166744, 105.0078781712208 21.86127484024044",
        description="Case Study in the Sông Mã district",
        preview_image="/static/song_ma.png",
        acl_read=uuids.values()
    )

    example_scenario_5 = Scenario(
        name="Malaysia",
        bbox="117.8925208474501 3.9325057026643435, 119.10843668692229 4.873028452279115",
        description="Case Study in Malaysia",
        preview_image="/static/malaysia.png",
        acl_read=uuids.values()
    )

    sess.add_all([
        example_scenario_2,
        example_scenario_3,
        example_scenario_4,
        example_scenario_5,
    ])
    sess.commit()


def add_example_processes(sess: Session, uuids: Dict[str, str]):
    example_process_1 = Process(
        name="DHI SAVI",
        deployment_id=UUID("0b206515-cdda-480c-9e4f-39d35b49f74f"),
        description="DHI SAVI STUFF",
        scenario_id=4,
        parameters={
            "$schema": "http://json-schema.org/draft-07/schema#",
            "title": "Generated schema for Root",
            "type": "object",
            "properties": {
                "rangestart": {
                    "type": "string"
                },
                "rangeend": {
                    "type": "string"
                },
                "spatial_extent": {
                    "type": "object",
                    "properties": {
                        "west": {
                            "type": "number"
                        },
                        "south": {
                            "type": "number"
                        },
                        "east": {
                            "type": "number"
                        },
                        "north": {
                            "type": "number"
                        },
                        "crs": {
                            "type": "string"
                        }
                    },
                    "required": [
                        "west",
                        "south",
                        "east",
                        "north",
                        "crs"
                    ]
                }
            },
            "required": [
                "rangestart",
                "rangeend",
                "spatial_extent"
            ]
        },
        acl_read=uuids.values()
    )
    example_process_2 = Process(
        name="Spectral Recovery",
        deployment_id=UUID("78aa3f3d-5ab4-42fb-bed1-63a0febd8b4f"),
        description="Based on the calculation of spectral index values on an annual basis using targeted best available pixels (BAPs) `(White et al., 2014). Theil Sen regression is the basis for deriving a trend for the selected spectral index. ",
        scenario_id=2,
        parameters={},
        acl_read=uuids.values()
    )

    sess.add_all([
        example_process_1,
        example_process_2
    ])
    sess.commit()


def add_example_timeseries(sess: Session, uuids: Dict[str, str]):
    example_ts_1 = Timeseries(
        scenario_id=3,
        name="Small Scale Demonstration 1 - Lebanon",
        description="Example for small scale demonstration",
        bucket="",
        process=1,
        acl_read=uuids.values()
    )
    example_ts_2 = Timeseries(
        scenario_id=4,
        name="Small Scale Demonstration 1 - Vietnam",
        description="Example for small scale demonstration",
        bucket="",
        process=2,
        acl_read=uuids.values()
    )

    sess.add_all([
        example_ts_1,
        example_ts_2
    ])
    sess.commit()


@click.command()
def account_info():
    cdse_client_id = click.prompt('cdse_client_id')
    cdse_client_secret = click.prompt('cdse_client_secret')
    conn = openeo.connect("https://openeofed.dataspace.copernicus.eu/").authenticate_oidc_client_credentials(
        client_id=cdse_client_id,
        client_secret=cdse_client_secret,
        provider_id="CDSE"
    )
    print(conn.list_jobs())
    print(conn.job('cdse-j-2603091516274995bb731b6653ea1c05').describe_job())


@click.command()
@click.option('--deployment-name', required=True, help='Name of the deployment (e.g., n52-test).')
@click.option('--image-name', required=True, help='Name of the Docker image (e.g., n52).')
@click.option('--image-tag', required=True, help='Tag for the Docker image (e.g., v4).')
@click.option('--dockerfile', required=True, help='Filename of the Dockerfile relative to `./eo/algorithms/` (e.g., n52/n52.Dockerfile)')
@click.option('--src-base', required=True, help='Build argument for ALGORITHM_BASE (e.g., n52.main).')
def deploy(
        deployment_name,
        image_name,
        image_tag,
        dockerfile,
        src_base
):
    from eo.algorithms.prefect_wrapper import run_algorithm

    dep = run_algorithm.to_deployment(name=deployment_name, _sync=True)
    dep.entrypoint = "prefect_wrapper.py:run_algorithm"
    deploy_to_prefect(
        dep,
        work_pool_name="docker",
        image=DockerImage(
            name=image_name,
            tag=image_tag,
            context=pathlib.Path.cwd().joinpath("eo/algorithms/"),
            dockerfile=dockerfile,
            quiet=False,
            stream_progress_to=sys.stdout,
            buildargs={"ALGORITHM_BASE": src_base},
        ),
        push=False,
        build=True
    )


@click.command()
def init_secrets():
    Secret(value=os.environ.get("ECCO_S3_URL")).save("ecco-s3-url", overwrite=True, _sync=True)
    Secret(value=os.environ.get("ECCO_S3_ACCESS_KEY")).save("ecco-s3-access-key", overwrite=True, _sync=True)
    Secret(value=os.environ.get("ECCO_S3_SECRET_KEY")).save("ecco-s3-secret-key", overwrite=True, _sync=True)

    cdse_client_id = click.prompt('cdse_client_id')
    cdse_client_secret = click.prompt('cdse_client_secret')
    Secret(value=cdse_client_id).save("ecco-openeo-client-id", overwrite=True, _sync=True)
    Secret(value=cdse_client_secret).save("ecco-openeo-client-secret", overwrite=True, _sync=True)


@click.command()
def s3_list():
    bucket_name = click.prompt('bucket_name')
    for obj in s3.list_objects(bucket_name, recursive=True):
        print(obj)


@click.command()
def s3_dl():
    bucket_name = click.prompt('bucket_name')
    filename = click.prompt('filename')
    response = s3.get_object(
        bucket_name,
        filename,
    )
    print(json.loads(response.data))


@click.command()
def s3_upload():
    bucket_name = click.prompt('bucket_name')
    filename = click.prompt('filename (with path)')
    upload_file_to_s3(bucket_name, filename)


def upload_file_to_s3(bucket: str, name: str) -> None:
    print(f"Start uploading file to s3: {name}")
    with open(name, 'rb') as file:
        # Find out file size
        file.seek(0, os.SEEK_END)
        size = file.tell()
        file.seek(0)
        s3.put_object(bucket, name, file, size)
    print(f"Done uploading file to s3: {name}")


cli.add_command(recreate_db)
cli.add_command(adddata)
cli.add_command(account_info)
cli.add_command(init_secrets)
cli.add_command(deploy)
cli.add_command(s3_list)
cli.add_command(s3_upload)
cli.add_command(s3_dl)

if __name__ == '__main__':
    cli()
