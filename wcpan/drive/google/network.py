import asyncio
import contextlib as cl
import enum
import functools as ft
import json
import math
import random
from typing import (Any, AsyncGenerator, AsyncIterator, Callable, Dict,
                    Generator, List, Optional, Text, Tuple, Union)
import urllib.parse as up

import aiohttp
from wcpan.logger import DEBUG, EXCEPTION, INFO, WARNING

from .util import GoogleDriveError, Settings


BACKOFF_FACTOR = 2
BACKOFF_STATUSES = set(('403', '429', '500', '502', '503', '504'))

ContentProducer = Callable[[], AsyncGenerator[bytes, None]]
ReadableContent = Union[bytes, ContentProducer]


def network_timeout(network_method):
    @ft.wraps(network_method)
    async def wrapper(self, *args, **kwargs):
        return await asyncio.wait_for(network_method(self, *args, **kwargs),
                                      self._timeout)
    return wrapper


class Network(object):

    def __init__(self, settings: Settings, timeout: int) -> None:
        self._settings = settings
        self._timeout = timeout
        self._backoff_level = 0
        self._session = None
        self._oauth = None
        self._raii = None

    async def __aenter__(self) -> 'Network':
        async with cl.AsyncExitStack() as stack:
            self._session = await stack.enter_async_context(
                aiohttp.ClientSession())
            self._oauth = await stack.enter_async_context(
                OAuth2Manager(self._session, self._settings))
            self._raii = stack.pop_all()

        return self

    async def __aexit__(self, type_, value, traceback) -> bool:
        await self._raii.aclose()
        self._backoff_level = 0
        self._session = None
        self._oauth = None
        self._raii = None

    @network_timeout
    async def fetch(self,
        method: Text,
        url: Text,
        args: Dict[Text, Any] = None,
        headers: Dict[Text, Text] = None,
        body: ReadableContent = None,
    ) -> 'JSONResponse':
        while True:
            kwargs = await self._prepare_kwargs(method, url, args, headers,
                                                body)

            try:
                response = await self._request_loop(kwargs)
            except aiohttp.ClientConnectionError as e:
                continue

            try:
                return await to_json_response(response)
            except aiohttp.ClientConnectionError as e:
                # The server recived the request, but reading body failed.
                # We are in a broken state, just let client to handle it.
                raise NetworkError() from e

    # NOTE Unlike download, upload cannot set timeout to the whole method.
    # Instead we should set timeout on body callback.
    async def upload(self,
        method: Text,
        url: Text,
        args: Dict[Text, Any] = None,
        headers: Dict[Text, Text] = None,
        body: ReadableContent = None,
    ) -> 'JSONResponse':
        kwargs = await self._prepare_kwargs(method, url, args, headers, body)
        kwargs['timeout'] = 0.0

        try:
            response = await self._request_loop(kwargs)
        except aiohttp.ClientConnectionError as e:
            raise NetworkError() from e

        return await to_json_response(response)

    @network_timeout
    async def download(self,
        method: Text,
        url: Text,
        args: Dict[Text, Any] = None,
        headers: Dict[Text, Text] = None,
        body: ReadableContent = None,
    ) -> 'StreamResponse':
        while True:
            kwargs = await self._prepare_kwargs(method, url, args, headers,
                                                body)
            kwargs['timeout'] = 0.0

            try:
                response = await self._request_loop(kwargs)
            except aiohttp.ClientConnectionError as e:
                continue

            return StreamResponse(response, self._timeout)

    async def _request_loop(self,
        kwargs: Dict[Text, Any],
    ) -> aiohttp.ClientResponse:
        while True:
            await self._wait_backoff()

            try:
                response = await self._session.request(**kwargs)
            except aiohttp.ClientConnectionError as e:
                self._adjust_backoff_level(True)
                raise

            status = str(response.status)
            rv = await self._check_status(status, response)
            if rv == Status.OK:
                return response
            if rv == Status.REFRESH:
                await self._oauth.renew_token()
                await self._update_token_header(kwargs['headers'])
                continue
            if rv == Status.BACKOFF:
                continue

            json_ = await response.json()
            raise ResponseError(status, response, json_)

    async def _prepare_kwargs(self,
        method: Text,
        url: Text,
        args: Optional[Dict[Text, Any]],
        headers: Optional[Dict[Text, Text]],
        body: Optional[ReadableContent],
    ) -> Dict[Text, Any]:
        kwargs = {
            'method': method,
            'url': url,
            'headers': await self._prepare_headers(headers),
        }
        if args is not None:
            kwargs['params'] = list(normalize_query_string(args))
        if body is not None:
            kwargs['data'] = body if not callable(body) else body()
        return kwargs

    async def _prepare_headers(self,
        headers: Optional[Dict[Text, Text]],
    ) -> Dict[Text, Text]:
        if headers is None:
            h = {}
        else:
            h = {k: v if isinstance(v, (bytes, str)) or v is None else str(v)
                 for k, v in headers.items()}
        await self._update_token_header(h)
        return h

    async def _update_token_header(self, headers: Dict[Text, Text]) -> None:
        token = await self._oauth.get_access_token()
        headers['Authorization'] = f'Bearer {token}'

    async def _check_status(self,
        status: Text,
        response: aiohttp.ClientResponse,
    ) -> 'Status':
        backoff = await backoff_needed(status, response)
        self._adjust_backoff_level(backoff)
        if backoff:
            # rate limit error, too many request, server error
            return Status.BACKOFF

        # normal response
        if status[0] in ('1', '2', '3'):
            return Status.OK

        # need to refresh access token
        if status == '401':
            return Status.REFRESH

        # otherwise it is an error
        return Status.UNKNOWN

    def _adjust_backoff_level(self, backoff: bool) -> None:
        if backoff:
            self._backoff_level = min(self._backoff_level + 2, 10)
        else:
            self._backoff_level = max(self._backoff_level - 1, 0)

    async def _wait_backoff(self) -> None:
        if self._backoff_level <= 0:
            return
        seed = random.random()
        power = 2 ** self._backoff_level
        s_delay = math.floor(seed * power * BACKOFF_FACTOR)
        s_delay = min(100, s_delay)
        DEBUG('wcpan.drive.google') << 'backoff for' << s_delay
        await asyncio.sleep(s_delay)


class Request(object):

    def __init__(self, request: aiohttp.RequestInfo) -> None:
        self._request = request

    @property
    def uri(self) -> Text:
        return self._request.url

    @property
    def method(self) -> Text:
        return self._request.method

    @property
    def headers(self) -> Dict[Text, Text]:
        return self._request.headers


class Response(object):

    def __init__(self, response: aiohttp.ClientResponse) -> None:
        self._response = response
        self._status = str(response.status)
        self._request = Request(response.request_info)

    @property
    def status(self):
        return self._status

    def get_header(self, key: Text) -> Text:
        h = self._response.headers.getall(key)
        return None if not h else h[0]


class JSONResponse(Response):

    def __init__(self,
        response: aiohttp.ClientResponse,
        json_: Dict[Text, Any],
    ) -> None:
        super().__init__(response)
        self._json = json_

    @property
    def json(self) -> Dict[Text, Any]:
        return self._json


class StreamResponse(Response):

    def __init__(self, response: aiohttp.ClientResponse, timeout: int) -> None:
        super().__init__(response)
        self._timeout = timeout

    async def __aenter__(self) -> 'StreamResponse':
        await self._response.__aenter__()
        return self

    async def __aexit__(self, type_, exc, tb) -> bool:
        await self._response.__aexit__(type_, exc, tb)

    async def chunks(self) -> AsyncIterator[bytes]:
        g = self._response.content.iter_any()
        while True:
            try:
                v = await asyncio.wait_for(g.__anext__(), self._timeout)
            except StopAsyncIteration:
                break
            yield v

    async def read(self, length):
        return await self._response.content.read(length)


async def to_json_response(response: aiohttp.ClientResponse) -> JSONResponse:
    async with response:
        if response.content_type == 'application/json':
            json_ = await response.json()
        else:
            json_ = await response.text()
    return JSONResponse(response, json_)


class ResponseError(GoogleDriveError):

    def __init__(self,
        status: Text,
        response: aiohttp.ClientResponse,
        json_: Dict[Text, Any],
    ) -> None:
        self._status = status
        self._response = response
        self._message = f'{self.status} {self._response.reason} - {json_}'
        self._json = json_

    def __str__(self) -> Text:
        return self._message

    @property
    def status(self) -> Text:
        return self._status

    @property
    def json(self) -> Any:
        return self._json


class NetworkError(GoogleDriveError):
    pass


class AuthenticationError(GoogleDriveError):
    pass


class OAuth2Manager(object):

    def __init__(self,
        session: aiohttp.ClientSession,
        settings: Settings,
    ) -> None:
        self._session = session
        self._settings = settings
        self._lock = asyncio.Condition()
        self._refreshing = False
        self._error = False
        self._oauth = None
        self._raii = None

    async def __aenter__(self) -> 'OAuth2Manager':
        oauth2_info = await self._settings.load_oauth2_info()

        async with cl.AsyncExitStack() as stack:
            self._oauth = await stack.enter_async_context(
                CommandLineGoogleDriveOAuth2(
                    self._session,
                    oauth2_info['client_id'],
                    oauth2_info['client_secret'],
                    oauth2_info['redirect_uri'],
                    oauth2_info['access_token'],
                    oauth2_info['refresh_token'],
                ))
            await self._settings.save_oauth2_info(self._oauth.access_token,
                                                  self._oauth.refresh_token)
            self._raii = stack.pop_all()

        return self

    async def __aexit__(self, type_, exc, tb) -> bool:
        await self._raii.aclose()
        self._raii = None
        self._oauth = None
        self._error = False
        self._refreshing = False

    async def get_access_token(self) -> Text:
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
                await self._settings.save_oauth2_info(self._oauth.access_token,
                                                      self._oauth.refresh_token)
            except Exception as e:
                EXCEPTION('wcpan.drive.google', e) << 'error on refresh token'
                self._error = True
                raise
            self._error = False

        DEBUG('wcpan.drive.google') << 'refresh access token'

    @cl.asynccontextmanager
    async def _guard(self):
        self._refreshing = True
        try:
            yield
        finally:
            self._refreshing = False
            async with self._lock:
                self._lock.notify_all()


class CommandLineGoogleDriveOAuth2(object):

    def __init__(self,
        session: aiohttp.ClientSession,
        client_id: Text,
        client_secret: Text,
        redirect_uri: Text,
        access_token: Text = None,
        refresh_token: Text = None
    ) -> None:
        self._session = session
        self._client_id = client_id
        self._client_secret = client_secret
        self._redirect_uri = redirect_uri
        self._access_token = access_token
        self._refresh_token = refresh_token

    async def __aenter__(self) -> 'CommandLineGoogleDriveOAuth2':
        if self._access_token is None:
            await self._fetch_access_token()
        return self

    async def __aexit__(self, type_, value, traceback) -> bool:
        pass

    @property
    def access_token(self) -> Text:
        assert self._access_token is not None
        return self._access_token

    @property
    def refresh_token(self) -> Union[Text, None]:
        return self._refresh_token

    async def refresh(self) -> None:
        headers = {
            'Content-Type': 'application/x-www-form-urlencoded',
        }
        body = up.urlencode({
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

    def _save_token(self, token: Dict[Text, Any]) -> None:
        self._access_token = token['access_token']
        if 'refresh_token' in token:
            self._refresh_token = token['refresh_token']

    async def _fetch_access_token(self) -> None:
        # get code on success
        code = await self._authorize_redirect()
        token = await self._get_authenticated_user(code=code)
        self._save_token(token)

    async def _authorize_redirect(self) -> Text:
        kwargs = {
            'redirect_uri': self._redirect_uri,
            'client_id': self._client_id,
            'response_type': 'code',
            'scope': ' '.join(self.scopes),
        }

        url = up.urlparse(self.oauth_authorize_url)
        url = up.urlunparse((
            url[0],
            url[1],
            url[2],
            url[3],
            up.urlencode(kwargs),
            url[5],
        ))
        return await self.redirect(url)

    # NOTE Google only
    @property
    def oauth_authorize_url(self) -> Text:
        return 'https://accounts.google.com/o/oauth2/auth'

    # NOTE Google only
    @property
    def oauth_access_token_url(self) -> Text:
        return 'https://accounts.google.com/o/oauth2/token'

    # NOTE Google only
    @property
    def scopes(self) -> List[Text]:
        return [
            'https://www.googleapis.com/auth/drive',
        ]

    # NOTE Google only?
    async def _get_authenticated_user(self, code: Text) -> Dict[Text, Any]:
        headers = {'Content-Type': 'application/x-www-form-urlencoded'}
        body = up.urlencode({
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
    async def redirect(self, url: Text) -> Text:
        print(url)
        return input().strip()


class Status(enum.Enum):

    OK = enum.auto()
    REFRESH = enum.auto()
    BACKOFF = enum.auto()
    UNKNOWN = enum.auto()


async def backoff_needed(
    status: Text,
    response: aiohttp.ClientResponse,
) -> bool:
    if status not in BACKOFF_STATUSES:
        return False

    # not all 403 errors are rate limit error
    if status == '403':
        msg = await response.json()
        if not msg:
            # undefined behavior, probably a server problem, better backoff
            WARNING('wcpan.drive.google') << '403 with empty error message'
            return True
        domain = msg['error']['errors'][0]['domain']
        if domain != 'usageLimits':
            return False

    return True


def normalize_query_string(
    qs: Dict[Text, Any],
) -> Generator[Tuple[Text, Text], None, None]:
    for key, value in qs.items():
        if isinstance(value, bool):
            value = 'true' if value else 'false'
        elif isinstance(value, (int, float)):
            value = str(value)
        elif not isinstance(value, str):
            raise ValueError('unknown type in query string')
        yield key, value
