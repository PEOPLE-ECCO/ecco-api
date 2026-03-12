from dataclasses import dataclass, field

import aiohttp
import jwt
from hypercorn.typing import ASGIFramework
from jwt import PyJWKClient


@dataclass
class AuthConfig:
    client_id: str
    oidc_server: str
    whitelist_paths: [str] = field(default_factory=lambda: [])  # Paths that are publicly accessible without any form of authentication


class AuthMiddleware:
    __jwks_client: PyJWKClient = None

    def __init__(self, asgi: ASGIFramework, config: AuthConfig):
        self.asgi = asgi
        self.config = config

    async def startup(self) -> None:
        print("starting AuthMiddleware")

    async def __call__(self, scope, receive, send):
        scope["uuid"] = ["52000000-0000-0000-0000-000000000052"]
        return await self.asgi(scope, receive, send)

        if scope["type"] == "lifespan":
            return await self.asgi(scope, receive, send)
        if scope["method"] == "OPTIONS":
            return await self.asgi(scope, receive, send)
        if scope["path"] in self.config.whitelist_paths:
            return await self.asgi(scope, receive, send)

        if not self.__jwks_client:
            await self.init_oidc()

        if "headers" not in scope:
            return await self.error_response(receive, send)

        token = None
        for header_key, header_val in scope['headers']:
            if header_key == b'authorization':
                token = header_val[7:]
                break

        if not token:
            return await self.error_response(receive, send)

        signing_key = self.__jwks_client.get_signing_key_from_jwt(token)
        try:
            decoded = jwt.decode(
                token,
                signing_key.key,
                audience="ecco-proxy",
                algorithms=["RS256"],
                issuer=self.config.oidc_server,
                options={
                    "verify_signature": True,
                    "verify_exp": True,
                    "verify_nbf": True,
                    "verify_iat": True,
                    "verify_aud": True,
                    "verify_iss": True,
                },
            )

            scope["username"] = decoded['name']
            scope["mail"] = decoded['email']
            scope["uuid"] = [decoded['sub']] + decoded['groups_uuids']

        except Exception as e:
            print(f"AUTH ERROR: {e}")
            return await self.error_response(receive, send)

        return await self.asgi(scope, receive, send)

    async def error_response(self, receive, send):
        await send({
            'type': 'http.response.start',
            'status': 401,
            'headers': [(b'content-length', b'0')],
        })
        await send({
            'type': 'http.response.body',
            'body': b'',
            'more_body': False,
        })

    async def init_oidc(self):
        async with aiohttp.ClientSession() as session:
            async with session.get(f"{self.config.oidc_server}/.well-known/openid-configuration") as config:
                oidc_config = await config.json()
                self.__jwks_client = jwt.PyJWKClient(oidc_config["jwks_uri"])
