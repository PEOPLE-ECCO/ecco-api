from dataclasses import dataclass

import aiohttp
import jwt
from quart import Quart
from quart_cors import cors
from auth.auth import AuthMiddleware, AuthConfig
from storage.routes import storage_bp

CONFIG = AuthConfig("ecco-proxy", "https://people-ecco.local/auth/realms/ecco")

APP = Quart(__name__)
APP = cors(APP, allow_origin="*")
# auth = AuthMiddleware(APP, CONFIG)
# APP.asgi_app = auth

APP.register_blueprint(storage_bp)

APP.STAC = []


@dataclass
class Config:
    s3_host: str = "localhost:9000"


@APP.route('/')
async def hello():
    return 'hello'


@APP.route('/stac')
async def stac():
    return APP.STAC


APP.run()
