from typing import TypedDict, Optional, Dict, Any, List
import asyncio
import contextlib
import json
import pathlib
import urllib.parse

import aiohttp
import yaml

from wcpan.logger import DEBUG, EXCEPTION

from .exceptions import AuthenticationError, CredentialFileError, TokenFileError


class OAuth2Info(TypedDict):

    client_id: str
    client_secret: str
    redirect_uri: str
    access_token: str
    refresh_token: str


FOLDER_MIME_TYPE = 'application/vnd.google-apps.folder'
OAUTH_TOKEN_VERSION = 1


class OAuth2Storage(object):

    def __init__(self,
        config_path: pathlib.Path,
        data_path: pathlib.Path,
    ) -> None:
        self._client_secret = config_path / 'client_secret.json'
        self._oauth_token = data_path / 'oauth_token.yaml'

    def load_oauth2_info(self) -> OAuth2Info:
        # load API key
        with self._client_secret.open('r') as fin:
            client = json.load(fin)
        try:
            client = client['installed']
            redirect_uri = client['redirect_uris'][0]
            client_id = client['client_id']
            client_secret = client['client_secret']
        except (KeyError, IndexError):
            raise CredentialFileError()

        # load refresh token
        if not self._oauth_token.is_file():
            access_token = None
            refresh_token = None
        else:
            with self._oauth_token.open('r') as fin:
                token = yaml.safe_load(fin)
            version = token.get('version', 0)
            if version != OAUTH_TOKEN_VERSION:
                raise TokenFileError(f'invalid token version: {version}')
            try:
                access_token = token['access_token']
                refresh_token = token['refresh_token']
            except KeyError:
                raise TokenFileError(f'invalid token format')

        return {
            'client_id': client_id,
            'client_secret': client_secret,
            'redirect_uri': redirect_uri,
            'access_token': access_token,
            'refresh_token': refresh_token,
        }

    def save_oauth2_info(self,
        access_token: str,
        refresh_token: str,
    ) -> None:
        token = {
            'version': OAUTH_TOKEN_VERSION,
            'access_token': access_token,
            'refresh_token': refresh_token,
        }

        # save refresh token
        with self._oauth_token.open('w') as fout:
            yaml.dump(token, fout, default_flow_style=False)


class OAuth2Manager(object):

    def __init__(self,
        session: aiohttp.ClientSession,
        storage: OAuth2Storage,
    ) -> None:
        self._session = session
        self._storage = storage
        self._lock = asyncio.Condition()
        self._refreshing = False
        self._error = False
        self._oauth = None
        self._raii = None

    async def __aenter__(self) -> 'OAuth2Manager':
        oauth2_info = self._storage.load_oauth2_info()

        async with contextlib.AsyncExitStack() as stack:
            self._oauth = await stack.enter_async_context(
                OAuth2CommandLineAuthenticator(
                    self._session,
                    oauth2_info['client_id'],
                    oauth2_info['client_secret'],
                    oauth2_info['redirect_uri'],
                    oauth2_info['access_token'],
                    oauth2_info['refresh_token'],
                ))
            self._storage.save_oauth2_info(
                self._oauth.access_token,
                self._oauth.refresh_token,
            )
            self._raii = stack.pop_all()

        return self

    async def __aexit__(self, type_, exc, tb) -> bool:
        await self._raii.aclose()
        self._raii = None
        self._oauth = None
        self._error = False
        self._refreshing = False

    async def get_access_token(self) -> str:
        if self._refreshing:
            async with self._lock:
                await self._lock.wait()
        if self._error:
            raise AuthenticationError()
        return self._oauth.access_token

    async def renew_token(self):
        if self._refreshing:
            async with self._lock:
                await self._lock.wait()
            return

        async with self._guard():
            try:
                await self._oauth.refresh()
                self._storage.save_oauth2_info(
                    self._oauth.access_token,
                    self._oauth.refresh_token,
                )
            except Exception as e:
                EXCEPTION('wcpan.drive.google', e) << 'error on refresh token'
                self._error = True
                raise
            self._error = False

        DEBUG('wcpan.drive.google') << 'refresh access token'

    @contextlib.asynccontextmanager
    async def _guard(self):
        self._refreshing = True
        try:
            yield
        finally:
            self._refreshing = False
            async with self._lock:
                self._lock.notify_all()


class OAuth2CommandLineAuthenticator(object):

    def __init__(self,
        session: aiohttp.ClientSession,
        client_id: str,
        client_secret: str,
        redirect_uri: str,
        access_token: str = None,
        refresh_token: str = None
    ) -> None:
        self._session = session
        self._client_id = client_id
        self._client_secret = client_secret
        self._redirect_uri = redirect_uri
        self._access_token = access_token
        self._refresh_token = refresh_token

    async def __aenter__(self) -> 'OAuth2CommandLineAuthenticator':
        if self._access_token is None:
            await self._fetch_access_token()
        return self

    async def __aexit__(self, type_, value, traceback) -> bool:
        pass

    @property
    def access_token(self) -> str:
        assert self._access_token is not None
        return self._access_token

    @property
    def refresh_token(self) -> Optional[str]:
        return self._refresh_token

    async def refresh(self) -> None:
        headers = {
            'Content-Type': 'application/x-www-form-urlencoded',
        }
        body = urllib.parse.urlencode({
            'client_id': self._client_id,
            'client_secret': self._client_secret,
            'refresh_token': self._refresh_token,
            'grant_type': 'refresh_token',
        })

        async with self._session.post(self.oauth_access_token_url,
                                      headers=headers, data=body) as response:
            response.raise_for_status()
            token = await response.json()
        self._save_token(token)

    def _save_token(self, token: Dict[str, Any]) -> None:
        self._access_token = token['access_token']
        if 'refresh_token' in token:
            self._refresh_token = token['refresh_token']

    async def _fetch_access_token(self) -> None:
        # get code on success
        code = await self._authorize_redirect()
        token = await self._get_authenticated_user(code=code)
        self._save_token(token)

    async def _authorize_redirect(self) -> str:
        kwargs = {
            'redirect_uri': self._redirect_uri,
            'client_id': self._client_id,
            'response_type': 'code',
            'scope': ' '.join(self.scopes),
        }

        url = urllib.parse.urlparse(self.oauth_authorize_url)
        url = urllib.parse.urlunparse((
            url[0],
            url[1],
            url[2],
            url[3],
            urllib.parse.urlencode(kwargs),
            url[5],
        ))
        return await self.redirect(url)

    @property
    def oauth_authorize_url(self) -> str:
        return 'https://accounts.google.com/o/oauth2/auth'

    @property
    def oauth_access_token_url(self) -> str:
        return 'https://accounts.google.com/o/oauth2/token'

    @property
    def scopes(self) -> List[str]:
        return [
            'https://www.googleapis.com/auth/drive',
        ]

    async def _get_authenticated_user(self, code: str) -> Dict[str, Any]:
        headers = {'Content-Type': 'application/x-www-form-urlencoded'}
        body = urllib.parse.urlencode({
            'redirect_uri': self._redirect_uri,
            'code': code,
            'client_id': self._client_id,
            'client_secret': self._client_secret,
            'grant_type': 'authorization_code',
        })
        async with self._session.post(self.oauth_access_token_url,
                                      headers=headers, data=body) as response:
            response.raise_for_status()
            return await response.json()

    # NOTE Use case depends
    async def redirect(self, url: str) -> str:
        print(url)
        return input().strip()
