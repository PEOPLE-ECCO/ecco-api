import os

from minio import Minio
from quart import Quart, request
from quart_cors import cors
from sqlalchemy import select

from auth.auth import AuthConfig, AuthMiddleware
# from storage.routes import storage_bp
from stac.routes import stac_bp
from storage.db import engine, setup_db, DBSession, DBConfig
from storage.definitions import *

AUTH_CONFIG = AuthConfig("ecco-proxy", "https://people-ecco.dev.52north.org/auth/realms/people-ecco")

APP = Quart(__name__)

APP = cors(APP, allow_origin="*")
APP.asgi_app = AuthMiddleware(APP, AUTH_CONFIG)
APP.register_blueprint(stac_bp)
# APP.register_blueprint(storage_bp)
APP.url_map.strict_slashes = True

APP.config["root"] = os.environ.get("ECCO_HOST", "people-ecco.dev.52north.org")

APP.config["S3"] = Minio(
    os.environ.get("ECCO_S3_URL", "192.168.52.212:9000"),
    secure=False,
    access_key=os.environ.get("ECCO_S3_ACCESS_KEY", "asdf"),
    secret_key=os.environ.get("ECCO_S3_SECRET_KEY", "asdfff")
)


@APP.before_serving
def setup():
    eng = engine(DBConfig(
        host=os.environ.get("ECCO_DB_HOST", "localhost"),
        port=int(os.environ.get("ECCO_DB_PORT", 5431)),
        user=os.environ.get("ECCO_DB_USER", "n52-prod-postgres"),
        password=os.environ.get("ECCO_DB_PASSWORD", "bAqU%4weuu3wy4WXT!T8kcn3v5%W^hB^"),
        dbname=os.environ.get("ECCO_DB_DBNAME", "ecco")
    ))
    setup_db(eng)


@APP.get('/processes/')
async def get_processes():
    with DBSession() as sess:
        return [a.as_dict() for a in sess.scalars(select(Process))]


@APP.get('/scenarios/')
async def get_scenarios():
    with DBSession() as sess:
        return [a.as_dict() for a in sess.scalars(select(Scenario))]


@APP.get('/scenarios/<int:scenario_id>/')
async def get_scenarios_by_id(scenario_id: int):
    with DBSession() as sess:
        return sess.scalar(select(Scenario, scenario_id)).as_dict()


@APP.get('/scenarios/<int:scenario_id>/timeseries/')
async def get_jobs(scenario_id: int):
    print(f"running {scenario_id}")
    with DBSession() as sess:
        return [a.as_dict() for a in sess.scalars(select(Timeseries).where(Timeseries.scenario_id == scenario_id))]


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

APP.run(host="0.0.0.0", debug=True)

if __name__ == "__main__":
    APP.run(host="0.0.0.0", debug=True)
