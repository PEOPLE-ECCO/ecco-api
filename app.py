import aiohttp
import jwt
from quart import Quart
from quart_cors import cors

from api.auth import AuthMiddleware, AuthConfig

CONFIG = AuthConfig("ecco-proxy", "https://people-ecco.local/auth/realms/ecco")

APP = Quart(__name__)
APP = cors(APP)
auth = AuthMiddleware(APP, CONFIG)
APP.asgi_app = auth


@APP.route('/')
async def hello():
    return 'hello'


APP.run()
