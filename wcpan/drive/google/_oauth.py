from asyncio import Condition
from contextlib import asynccontextmanager
from logging import getLogger
from pathlib import Path
from typing import TypedDict
from urllib.parse import urlencode, urlunparse, urlparse, parse_qs
import json

from aiohttp import ClientSession
from wcpan.drive.core.exceptions import UnauthorizedError

from .exceptions import AuthenticationError, CredentialFileError, TokenFileError


OAUTH_TOKEN_VERSION = 1
_OAUTH_SCOPES = [
    "https://www.googleapis.com/auth/drive",
]


class OAuth2Config(TypedDict):
    client_id: str
    client_secret: str
    redirect_uri: str
    auth_uri: str
    token_uri: str


class OAuth2Token(TypedDict):
    access_token: str
    refresh_token: str


class OAuth2Storage(object):
    def __init__(
        self,
        *,
        client_secret: Path,
        oauth_token: Path,
    ) -> None:
        self._client_secret = client_secret
        self._oauth_token = oauth_token

    def load_oauth2_config(self) -> OAuth2Config:
        with self._client_secret.open("r") as fin:
            config = json.load(fin)

        if "web" not in config:
            raise CredentialFileError(
                "credential file not supported (only supports `web`)"
            )

        web = config["web"]
        return _load_web(web)

    def load_oauth2_token(self) -> OAuth2Token:
        if not self._oauth_token.is_file():
            return {
                "access_token": "",
                "refresh_token": "",
            }

        with self._oauth_token.open("r") as fin:
            token = json.load(fin)
        version = token.get("version", 0)
        if version != OAUTH_TOKEN_VERSION:
            raise TokenFileError(f"invalid token version: {version}")
        try:
            access_token = token["access_token"]
            refresh_token = token["refresh_token"]
        except KeyError:
            raise TokenFileError(f"invalid token format")
        return {
            "access_token": access_token,
            "refresh_token": refresh_token,
        }

    def save_oauth2_token(
        self,
        access_token: str,
        refresh_token: str,
    ) -> None:
        token = {
            "version": OAUTH_TOKEN_VERSION,
            "access_token": access_token,
            "refresh_token": refresh_token,
        }

        # save refresh token
        with self._oauth_token.open("w") as fout:
            json.dump(token, fout)


class OAuth2Manager(object):
    def __init__(self, storage: OAuth2Storage) -> None:
        self._storage = storage
        self._lock = Condition()
        self._refreshing = False
        self._error = False
        self._oauth2_config: OAuth2Config = self._storage.load_oauth2_config()
        self._oauth2_token: OAuth2Token = self._storage.load_oauth2_token()

    @property
    def access_token(self) -> str:
        return self._oauth2_token.get("access_token", None)

    @property
    def refresh_token(self) -> str:
        return self._oauth2_token.get("refresh_token", None)

    def build_authorization_url(self) -> str:
        kwargs = {
            "redirect_uri": self._oauth2_config["redirect_uri"],
            "client_id": self._oauth2_config["client_id"],
            "response_type": "code",
            # Essential for getting refresh token.
            "access_type": "offline",
            # Essential for getting refresh token **everytime**.
            # See https://github.com/googleapis/google-api-python-client/issues/213 .
            "prompt": "consent",
            "scope": " ".join(_OAUTH_SCOPES),
        }
        url = urlparse(self._oauth2_config["auth_uri"])
        url = urlunparse(
            (
                url[0],
                url[1],
                url[2],
                url[3],
                urlencode(kwargs),
                url[5],
            )
        )
        return url

    async def set_authenticated_token(
        self,
        session: ClientSession,
        code: str,
    ) -> None:
        code = _parse_authorized_code(code)
        headers = {"Content-Type": "application/x-www-form-urlencoded"}
        body = urlencode(
            {
                "redirect_uri": self._oauth2_config["redirect_uri"],
                "code": code,
                "client_id": self._oauth2_config["client_id"],
                "client_secret": self._oauth2_config["client_secret"],
                "grant_type": "authorization_code",
            }
        )
        async with session.post(
            self._oauth2_config["token_uri"], headers=headers, data=body
        ) as response:
            response.raise_for_status()
            token = await response.json()
        self._save_token(token)

    async def safe_get_access_token(self) -> str:
        if self._refreshing:
            async with self._lock:
                await self._lock.wait()
        if self._error:
            raise AuthenticationError()
        return self.access_token

    async def renew_token(self, session: ClientSession) -> None:
        if self._refreshing:
            async with self._lock:
                await self._lock.wait()
            return

        if not self.access_token:
            raise UnauthorizedError()

        if not self.refresh_token:
            raise UnauthorizedError()

        async with self._guard():
            try:
                await self._refresh(session)
            except Exception:
                getLogger(__name__).exception("error on refresh token")
                self._error = True
                raise
            self._error = False

        getLogger(__name__).debug("refresh access token")

    @asynccontextmanager
    async def _guard(self):
        self._refreshing = True
        try:
            yield
        finally:
            self._refreshing = False
            async with self._lock:
                self._lock.notify_all()

    def _save_token(self, token: dict[str, str]) -> None:
        self._oauth2_token["access_token"] = token["access_token"]
        if "refresh_token" in token:
            self._oauth2_token["refresh_token"] = token["refresh_token"]
        self._storage.save_oauth2_token(self.access_token, self.refresh_token)

    async def _refresh(self, session: ClientSession):
        headers = {
            "Content-Type": "application/x-www-form-urlencoded",
        }
        body = urlencode(
            {
                "client_id": self._oauth2_config["client_id"],
                "client_secret": self._oauth2_config["client_secret"],
                "refresh_token": self.refresh_token,
                "grant_type": "refresh_token",
            }
        )

        async with session.post(
            self._oauth2_config["token_uri"], headers=headers, data=body
        ) as response:
            response.raise_for_status()
            token = await response.json()
        self._save_token(token)


def _load_web(web: dict[str, str]) -> OAuth2Config:
    return {
        "client_id": web["client_id"],
        "client_secret": web["client_secret"],
        "redirect_uri": web["redirect_uris"][0],
        "auth_uri": web["auth_uri"],
        "token_uri": web["token_uri"],
    }


def _parse_authorized_code(code: str) -> str:
    if not code.startswith("http"):
        return code

    url = urlparse(code)
    query = url.query
    params = parse_qs(query)
    code = params["code"][0]
    return code
