import asyncio
import json
import os
from datetime import timedelta

from celery import Celery, Task
from dotenv import load_dotenv
from hypercorn.middleware import ProxyFixMiddleware
from minio import Minio, S3Error
from quart import Quart, request, current_app
from quart_cors import cors
from sqlalchemy import select

from auth.auth import AuthConfig, AuthMiddleware
from eo.api import *
from stac.routes import stac_bp
from storage.db import connect_db, DBSession, DBConfig
from storage.definitions import *
from tasks import schedule_job

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
APP.url_map.strict_slashes = True

APP.config["root"] = os.environ.get("ECCO_HOST", "people-ecco.dev.52north.org")

APP.config["S3"] = Minio(
    os.environ.get("ECCO_S3_URL", "s3.people-ecco.dev.52north.org"),
    secure=True,
    access_key=os.environ.get("ECCO_S3_ACCESS_KEY", "aa"),
    secret_key=os.environ.get("ECCO_S3_SECRET_KEY", "aa")
)
# APP.config["S3"].trace_on(sys.stdout)

APP.config["CELERY"] = {
    "broker_url": os.environ.get("ECCO_REDIS_HOST", "redis://localhost"),
    "result_backend": os.environ.get("ECCO_REDIS_HOST", "redis://localhost"),
    "task_ignore_result": True
}

openeo: OpenEO = OpenEO(OpenEOConfig(
    backend=os.environ.get("ECCO_OPENEO_BACKEND", "openeo.vito.be"),
    oidc_provider=os.environ.get("ECCO_OPENEO_OIDC_PROVIDER", "terrascope"),
))
APP.config["openeo"] = openeo

db_config = DBConfig(
    host=os.environ.get("ECCO_DB_HOST", "localhost"),
    port=int(os.environ.get("ECCO_DB_PORT", 5431)),
    user=os.environ.get("ECCO_DB_USER", "aa"),
    password=os.environ.get("ECCO_DB_PASSWORD", "aa"),
    dbname=os.environ.get("ECCO_DB_DBNAME", "ecco")
)
APP.config["db"] = db_config


class QuartTask(Task):
    def __call__(self, *args: object, **kwargs: object) -> object:
        loop = asyncio.get_event_loop()

        async def fk():
            async with APP.app_context() as ctx:
                return self.run(*args, **kwargs)

        loop.run_until_complete(fk())


celery_app = Celery(APP.name, task_cls=QuartTask)
celery_app.config_from_object(APP.config["CELERY"])
celery_app.set_default()
APP.extensions["celery"] = celery_app


@APP.before_serving
async def setup():
    connect_db(db_config)


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
        scenario = sess.get(Scenario, scenario_id)
        return scenario.as_dict() if scenario else "", 404


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
        LOGGER.warn("TODO: check that this timeseries is actually valid")

        ts = Timeseries(
            scenario_id=scenario_id,
            name=input["name"],
            description=input["description"],
            process=input["process"],
            bucket=str(uuid.uuid4()),
            acl_read=scenario.acl_read
        )
        sess.add(ts)

        s3: Minio = current_app.config["S3"]
        s3.make_bucket(ts.bucket)

        sess.commit()
        return f"{ts.id}", 201


@APP.get('/scenarios/<int:scenario_id>/timeseries/<int:timeseries_id>/jobs')
async def get_jobs(scenario_id: int, timeseries_id: int):
    with DBSession() as sess:
        jobs = [a.as_dict() for a in sess.scalars(select(Job).where(Job.timeseries_id == timeseries_id))]
        return jobs


@APP.get('/scenarios/<int:scenario_id>/timeseries/<int:timeseries_id>/jobs/<int:job_id>')
async def get_job(scenario_id: int, timeseries_id: int, job_id: int):
    with DBSession() as sess:
        return sess.get(Job, job_id), 200, {'Content-Type': 'application/json'}


@APP.get('/scenarios/<int:scenario_id>/timeseries/<int:timeseries_id>/jobs/<int:job_id>/log')
def get_job_logs(scenario_id: int, timeseries_id: int, job_id: int):
    LOGGER.error("TODO: validate that Job exists")
    with DBSession() as sess:
        return sess.get(Job, job_id).log, 200, {'Content-Type': 'application/json'}


@APP.get('/scenarios/<int:scenario_id>/timeseries/<int:timeseries_id>/jobs/<int:job_id>/catalog')
async def get_job_catalog(scenario_id: int, timeseries_id: int, job_id: int):
    LOGGER.error("TODO: validate that Job exists")
    LOGGER.error("TODO: validate that Timeseries exists")
    with DBSession() as sess:
        job = sess.get(Job, job_id)
        ts = sess.get(Timeseries, timeseries_id)

        # Fetch catalog
        s3 = current_app.config["S3"]
        response = None
        try:
            response = s3.get_object(
                ts.bucket,
                str(job.id) + "/job-results.json",
            )
            catalog = json.loads(response.data)
        except S3Error as e:
            return "", 404, {'Content-Type': 'application/json'}
        finally:
            if response:
                response.release_conn()

        # Replace all links inside the catalog with presigned links
        for asset in catalog["assets"].values():
            asset["href"] = s3.presigned_get_object(
                ts.bucket,
                f"{job_id}/{asset['href'].split('/')[-1].split('?')[0]}",
                expires=timedelta(minutes=5)
            )

        if "links" in catalog:
            catalog["links"] = [o for o in catalog["links"] if o["rel"] not in ["item", "canonical", "self"]]

        return json.dumps(catalog), 200, {'Content-Type': 'application/json'}


@APP.post('/scenarios/<int:scenario_id>/timeseries/<int:timeseries_id>')
async def post_process_execution(scenario_id: int, timeseries_id: int):
    # 1. Store in local Job DB
    # 2. Start
    # 3. Create celery task for monitoring+downloading results once finished
    with DBSession() as sess:
        ts = sess.get(Timeseries, timeseries_id)
        if not ts: return "", 404

        job = Job(
            timeseries_id=timeseries_id,
            status=JobStatus.INIT,
            scheduleTime=datetime.datetime.now(),
            acl_read=ts.acl_read
        )
        sess.add(job)
        sess.commit()

        schedule_job.delay(job.id)
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
