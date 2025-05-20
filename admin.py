import datetime
import os
import random
from typing import Dict

import click
from sqlalchemy import Engine
from sqlalchemy.orm import Session

from storage.db import DBSession, recreate_tables, DBConfig, connect_db
from storage.definitions import Process, Scenario, Job, JobStatus, Timeseries


@click.group()
def cli():
    pass


def open() -> Engine:
    return connect_db(DBConfig(
        host=os.environ.get("ECCO_DB_HOST", "localhost"),
        port=int(os.environ.get("ECCO_DB_PORT", 5431)),
        user=os.environ.get("ECCO_DB_USER", ""),
        password=os.environ.get("ECCO_DB_PASSWORD", ""),
        dbname=os.environ.get("ECCO_DB_DBNAME", "ecco")
    ))


@click.command()
def initdb():
    click.echo('Initializing the database')
    engine = open()
    recreate_tables(engine)


@click.command()
def adddata():
    engine = open()
    with DBSession(admin=True, engine=engine) as sess:
        uuids = {
            "kc_garamba_uuid": "71919f81-6934-4cb3-8179-1354c788c618"
        }
        add_example_scenarios(sess, uuids)
        add_example_processes(sess, uuids)
        #add_example_timeseries(sess, uuids)
        #add_example_jobs(sess, uuids)


@click.command()
def dropdb():
    click.echo('Dropped the database')


cli.add_command(initdb)
cli.add_command(dropdb)
cli.add_command(adddata)


def add_example_scenarios(sess: Session, uuids: Dict[str, str]):
    example_scenario_1 = Scenario(
        name="Public Demo Scenario",
        description="Case Study at 52°North in Münster",
        preview_image="https://images.unsplash.com/photo-1581852017103-68ac65514cf7?q=80&w=2673&auto=format&fit=crop&ixlib=rb-4.0.3&ixid=M3wxMjA3fDB8MHxwaG90by1wYWdlfHx8fGVufDB8fHx8fA%3D%3D",
        acl_read=uuids.values()
    )

    example_scenario_2 = Scenario(
        name="Garamba National Park 1",
        description="Case Study at the GNP",
        preview_image="https://images.unsplash.com/photo-1557050543-4d5f4e07ef46?q=80&w=2664&auto=format&fit=crop&ixlib=rb-4.0.3&ixid=M3wxMjA3fDB8MHxwaG90by1wYWdlfHx8fGVufDB8fHx8fA%3D%3D",
        acl_read=uuids.values()
    )

    sess.add_all([
        example_scenario_1,
        example_scenario_2
    ])
    sess.commit()


def add_example_processes(sess: Session, uuids: Dict[str, str]):
    example_process_1 = Process(
        name="basic_ndvi",
        description="Calculates the basic NDVI",
        scenario_id=1,
        git_commit="2825818f7c8490094820fbba78db3d8052245cc6",
        git_repo="https://github.com/PEOPLE-ECCO/algorithms/",
        git_location="src/public_algorithms/common/ndvi",
        parameters={
            "bbox": {
                "type": "array"
            },
            "threshold": 0.7
        },
        acl_read=uuids.values()
    )

    example_process_2 = Process(
        name="open_water_surface",
        description="Calculates the percentage of open water in ",
        scenario_id=1,
        git_commit="4cbe4fb06d8bb3e8b28af345589aec3d30826cf6",
        git_repo="https://github.com/PEOPLE-ECCO/algorithms/",
        git_location="src/public_algorithms/water/openwatersurface",
        parameters={
            "bbox": {
                "type": "array"
            },
        },
        acl_read=uuids.values()
    )

    example_process_3 = Process(
        name="private_pixelcount",
        description="Counts all pixels with value x",
        scenario_id=2,
        git_commit="4cbe4fb06d8bb3e8b28af345589aec3d30826aaa",
        git_repo="https://github.com/PEOPLE-ECCO/algorithms/",
        git_location="src/private_algorithms/pixelcount",
        parameters={
            "bbox": {
                "type": "array"
            },
            "x": 838
        },
        acl_read=uuids.values()
    )

    sess.add_all([
        example_process_1,
        example_process_2,
        example_process_3
    ])
    sess.commit()


def add_example_timeseries(sess: Session, uuids: Dict[str, str]):
    example_ts_1 = Timeseries(
        scenario_id=1,
        name="test_ts_1",
        description="test_ts_1_description",
        catalog="/api/stac/catalog.json",
        acl_read=uuids.values()
    )
    example_ts_2 = Timeseries(
        scenario_id=2,
        name="test_ts_2",
        description="test_ts_2_description",
        catalog="/api/stac/catalog.json",
        acl_read=uuids.values()
    )
    example_ts_3 = Timeseries(
        scenario_id=1,
        name="test_ts_3",
        description="test_ts_3_description",
        catalog="/api/stac/catalog.json",
        acl_read=uuids.values()
    )

    sess.add_all([
        example_ts_1,
        example_ts_2,
        example_ts_3
    ])
    sess.commit()


def add_example_jobs(sess: Session, uuids: Dict[str, str]):
    example_job_1 = Job(
        timeseries_id=1,
        status=JobStatus.COMPLETED,
        scheduleTime=datetime.datetime.now() + datetime.timedelta(days=1200),
        executionTimeStart=datetime.datetime.now(),
        executionTimeEnd=datetime.datetime.now(),
        credits=random.randint(0, 1_000),
        log="test.txt",
        catalog="/api/stac/stac_2017-05_garamba.json",
        acl_read=uuids.values()
    )

    example_job_2 = Job(
        timeseries_id=1,
        status=JobStatus.RUNNING,
        scheduleTime=datetime.datetime.now() + datetime.timedelta(days=900),
        executionTimeStart=datetime.datetime.now(),
        executionTimeEnd=datetime.datetime.now(),
        credits=random.randint(0, 15),
        log="test.txt",
        catalog="/api/stac/stac_2018-05_garamba.json",
        acl_read=uuids.values()
    )

    example_job_5 = Job(
        timeseries_id=1,
        status=JobStatus.RUNNING,
        scheduleTime=datetime.datetime.now() + datetime.timedelta(days=900),
        executionTimeStart=datetime.datetime.now(),
        executionTimeEnd=datetime.datetime.now(),
        credits=random.randint(0, 15),
        log="test.txt",
        catalog="/api/stac/stac_2019-05_garamba.json",
        acl_read=uuids.values()
    )

    example_job_3 = Job(
        timeseries_id=2,
        status=JobStatus.RUNNING,
        scheduleTime=datetime.datetime.now() + datetime.timedelta(days=600),
        executionTimeStart=datetime.datetime.now(),
        executionTimeEnd=datetime.datetime.now(),
        credits=random.randint(0, 123123),
        log="test.txt",
        catalog="/api/stac/stac_2017-05_garamba.json",
        acl_read=uuids.values()
    )

    example_job_4 = Job(
        timeseries_id=3,
        status=JobStatus.RUNNING,
        scheduleTime=datetime.datetime.now() + datetime.timedelta(days=300),
        executionTimeStart=datetime.datetime.now(),
        executionTimeEnd=datetime.datetime.now(),
        credits=random.randint(0, 111),
        log="test.txt",
        catalog="/api/stac/stac_2019-05_garamba.json",
        acl_read=uuids.values()
    )

    sess.add_all([
        example_job_1,
        example_job_2,
        example_job_3,
        example_job_4,
        example_job_5
    ])
    sess.commit()


if __name__ == '__main__':
    cli()
