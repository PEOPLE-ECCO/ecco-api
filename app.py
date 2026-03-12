import json
import os
from datetime import timedelta
from uuid import UUID

from dotenv import load_dotenv
from hypercorn.middleware import ProxyFixMiddleware
from minio import Minio, S3Error
from prefect import get_client
from prefect.client.orchestration import PrefectClient
from prefect.client.schemas.filters import LogFilter
from prefect.deployments import run_deployment
from prefect.flow_engine import load_flow_run
from quart import Quart, request, current_app
from quart_cors import cors
from sqlalchemy import select, exists, and_

from auth.auth import AuthConfig, AuthMiddleware
from eo.api import *
from stac.routes import stac_bp
from storage.db import connect_db, DBSession, DBConfig
from storage.definitions import *

load_dotenv()

AUTH_CONFIG = AuthConfig("ecco-proxy",
                         "https://people-ecco.dev.52north.org/auth/realms/people-ecco",
                         ["/", "/openapi.yaml"]
                         )

APP = Quart(__name__)

APP: Quart = cors(APP, allow_origin="*")
APP.asgi_app = ProxyFixMiddleware(APP.asgi_app, mode="modern", trusted_hops=1)
APP.asgi_app = AuthMiddleware(APP.asgi_app, AUTH_CONFIG)

APP.register_blueprint(stac_bp)
# APP.register_blueprint(storage_bp)
APP.url_map.strict_slashes = False

APP.config["root"] = os.environ.get("ECCO_HOST", "people-ecco.dev.52north.org")

APP.config["S3"] = Minio(
    os.environ.get("ECCO_S3_URL", "s3.people-ecco.dev.52north.org"),
    secure=True,
    access_key=os.environ.get("ECCO_S3_ACCESS_KEY", "aa"),
    secret_key=os.environ.get("ECCO_S3_SECRET_KEY", "aa"),
    region="garage"
)
# APP.config["S3"].trace_on(sys.stdout)

db_config = DBConfig(
    host=os.environ.get("ECCO_DB_HOST", "localhost"),
    port=int(os.environ.get("ECCO_DB_PORT", 5431)),
    user=os.environ.get("ECCO_DB_USER", "aa"),
    password=os.environ.get("ECCO_DB_PASSWORD", "aa"),
    dbname=os.environ.get("ECCO_DB_DBNAME", "ecco")
)
APP.config["db"] = db_config


@APP.before_serving
async def setup():
    connect_db(db_config)
    # APP.add_background_task(init_deployments)


@APP.get('/static/<path:path>')
async def static_file(filename):
    return await APP.send_from_directory('static', path)


@APP.get('/')
async def redoc():
    return await APP.send_static_file("redoc.html")


@APP.get('/openapi.yaml')
async def openapi():
    return await APP.send_static_file("openapi.yaml")


@APP.get('/scenarios/')
async def get_scenarios():
    with DBSession() as sess:
        return [a.as_dict() for a in sess.scalars(select(Scenario))]


@APP.get('/scenarios/<int:scenario_id>')
async def get_scenarios_by_id(scenario_id: int):
    with DBSession() as sess:
        scenario: Scenario = sess.get(Scenario, scenario_id)
        if scenario:
            return scenario.as_dict()
        else:
            return "", 404


@APP.get('/scenarios/<int:scenario_id>/processes/')
async def get_processes(scenario_id: int):
    with DBSession() as sess:
        return [a.as_dict() for a in sess.scalars(select(Process).where(Process.scenario_id == scenario_id))]


@APP.get('/scenarios/<int:scenario_id>/timeseries/')
async def get_timeseries(scenario_id: int):
    with DBSession() as sess:
        return [a.as_dict() for a in sess.scalars(select(Timeseries).where(Timeseries.scenario_id == scenario_id))]


@APP.post('/scenarios/<int:scenario_id>/timeseries/')
async def post_timeseries(scenario_id: int):
    input = await request.get_json()

    with DBSession() as sess:
        scenario = sess.get(Scenario, scenario_id)
        if not scenario:
            return f"cannot find scenario {scenario_id}", 404

        # check process
        if not sess.query(exists(Process).where(and_(Process.id == input["process"]["id"], Process.scenario_id == scenario_id))).scalar():
            return "cannot find process in this scenario", 400

        # create timeseries
        ts = Timeseries(
            scenario_id=scenario_id,
            name=input["name"],
            description=input["description"],
            process=input["process"]["id"],
            bbox=f"{bbox[0]} {bbox[1]}, {bbox[2]} {bbox[3]}",
            geometry=func.ST_GeomFromGeoJSON(json.dumps(input["extent"]["geometry"]["geometry"])),
            acl_read=scenario.acl_read
        )
        sess.add(ts)
        sess.commit()
        return f"{ts.id}", 201


@APP.get('/timeseries/<int:timeseries_id>/jobs/')
async def get_jobs(timeseries_id: int):
    with DBSession() as sess:
        return [job_to_flow(j)
                for j in sess.scalars(select(Job).where(Job.timeseries_id == timeseries_id))]


def job_to_flow(job: Job):
    return load_flow_run(job.flow_run_id).model_dump(mode='json') | job.as_dict()


@APP.get('/jobs/<int:job_id>/')
async def get_job(job_id: int):
    with DBSession() as sess:
        job = sess.get(Job, job_id)
        if job:
            return job_to_flow(job)
        else:
            return "", 404


@APP.get('/jobs/<int:job_id>/log/')
async def get_job_logs(job_id: int):
    with DBSession() as sess:
        job = sess.get(Job, job_id)
    async with get_client() as client:
        c: PrefectClient = client
        log_filter = LogFilter(flow_run_id={"any_": [str(job.flow_run_id)]})
        logs = await c.read_logs(log_filter)
        if logs:
            return [l.model_dump(mode='json') for l in logs], 200, {'Content-Type': 'application/json'}
        else:
            return "", 404


@APP.get('/jobs/<int:job_id>/results')
async def get_job_results(job_id: int):
    with DBSession() as sess:
        job = sess.get(Job, job_id)
        if not job:
            return f"no job with id {job_id} found", 404

        # Fetch results
        s3 = current_app.config["S3"]
        response = None
        try:
            response = s3.get_object(
                job.flow_run_name,
                "stac.collection",
            )
            collection = json.loads(response.data)
        except S3Error as e:
            LOGGER.error(e)
            return f"", 404
        finally:
            if response:
                response.release_conn()

        # Replace all links inside the collection with presigned links
        for feature in collection['features']:
            for image in feature['assets'].values():
                image["href"] = s3.presigned_get_object(
                    job.flow_run_name,
                    image['href'],
                    expires=timedelta(minutes=120)
                )
        return json.dumps(collection), 200, {'Content-Type': 'application/json'}


@APP.post('/timeseries/<int:timeseries_id>/jobs/')
async def post_job(timeseries_id: int):
    input = await request.get_json()
    # 1. Store in local Job DB
    # 2. Start
    # 3. Create celery task for monitoring+downloading results once finished
    with DBSession() as sess:
        ts = sess.get(Timeseries, timeseries_id)
        if not ts: return "", 404

        process = sess.get(Process, ts.process)
        if not process: return "", 404

        user_query = select(User).where(User.kc_uuid == request.scope['uuid'])
        user: User = sess.scalar(user_query)
        if not user: return "", 404

        # get spatial_extent from timeseries

        # TODO: input validation
        params = {
            "parameters": input | {
                "spatial_extent": json.loads(ts.geom_geojson)
            }
        }
        flow_run = await run_deployment(
            name=process.deployment_id,
            parameters=params,
            timeout=0,
            _sync=False
        )

        job = Job(
            flow_run_name=flow_run.name,
            flow_run_id=flow_run.id,
            timeseries_id=timeseries_id,
            scheduleTime=datetime.datetime.now(),
            user_id=user.id,
            acl_read=ts.acl_read
        )
        sess.add(job)
        sess.commit()

        return f"{job.id}", 201


@APP.route('/user/')
async def get_or_create_user():
    with DBSession() as sess:
        user_query = select(User).where(User.kc_uuid == request.scope['uuid'])
        user = sess.scalar(user_query)
        if user is None:
            # Create a new user
            newuser = User(
                name=request.scope['username'],
                mail=request.scope['mail'],
                kc_uuid=request.scope['uuid'],
            )
            sess.add(newuser)
            sess.commit()
            return sess.scalar(user_query).as_dict()
        else:
            return user.as_dict()


if __name__ == "__main__":
    APP.run(host="0.0.0.0", debug=True)
