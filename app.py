import asyncio
import os

from celery import Celery, Task
from hypercorn.middleware import ProxyFixMiddleware
from minio import Minio
from quart import Quart, request
from quart_cors import cors
from sqlalchemy import select

from auth.auth import AuthConfig, AuthMiddleware
from eo.api import *
from stac.routes import stac_bp
from storage.db import connect_db, DBSession, DBConfig
from storage.definitions import *

AUTH_CONFIG = AuthConfig("ecco-proxy", "https://people-ecco.dev.52north.org/auth/realms/people-ecco")

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
    secret_key=os.environ.get("ECCO_S3_SECRET_KEY", "aaa")
)
# APP.config["S3"].trace_on(sys.stdout)

APP.config["CELERY"] = {
    "broker_url": os.environ.get("ECCO_REDIS_HOST", "redis://localhost"),
    "result_backend": os.environ.get("ECCO_REDIS_HOST", "redis://localhost"),
    "task_ignore_result": True
}

APP.config["openeo"] = OpenEO(OpenEOConfig(
    backend=os.environ.get("ECCO_OPENEO_BACKEND", "openeo.cloud"),
))


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
    connect_db(DBConfig(
        host=os.environ.get("ECCO_DB_HOST", "localhost"),
        port=int(os.environ.get("ECCO_DB_PORT", 5431)),
        user=os.environ.get("ECCO_DB_USER", "n52-prod-postgres"),
        password=os.environ.get("ECCO_DB_PASSWORD", "bAqU%4weuu3wy4WXT!T8kcn3v5%W^hB^"),
        dbname=os.environ.get("ECCO_DB_DBNAME", "ecco")
    ))


@APP.get('/scenarios/')
async def get_scenarios():
    with DBSession() as sess:
        return [a.as_dict() for a in sess.scalars(select(Scenario))]


@APP.get('/scenarios/<int:scenario_id>')
async def get_scenarios_by_id(scenario_id: int):
    with DBSession() as sess:
        return sess.scalar(select(Scenario, scenario_id)).as_dict()


@APP.get('/scenarios/<int:scenario_id>/processes/')
async def get_processes(scenario_id: int):
    with DBSession() as sess:
        return [a.as_dict() for a in sess.scalars(select(Process).where(Process.scenario_id == scenario_id))]


@APP.get('/scenarios/<int:scenario_id>/timeseries/')
async def get_timeseries(scenario_id: int):
    with DBSession() as sess:
        return [a.as_dict() for a in sess.scalars(select(Timeseries).where(Timeseries.scenario_id == scenario_id))]


@APP.post('/scenarios/<int:scenario_id>/processes/execution')
async def post_process_execution(scenario_id: int):
    # 1. Store in local Job DB
    # 2. Start
    # 3. Create celery task for monitoring+downloading results once finished
    with DBSession() as sess:
        return [a.as_dict() for a in sess.scalars(select(Process).where(Process.scenario_id == scenario_id))]


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
