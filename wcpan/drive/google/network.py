import asyncio
import contextlib
import enum
import functools
import json
import math
import random
from typing import (
    Any,
    AsyncGenerator,
    AsyncIterator,
    Callable,
    Dict,
    Generator,
    List,
    Optional,
    Tuple,
    Union,
)

import aiohttp
from wcpan.logger import DEBUG, EXCEPTION, INFO, WARNING

from .util import OAuth2Storage, OAuth2Manager
from .exceptions import (
    DownloadAbusiveFileError,
    InvalidAbuseFlagError,
    NetworkError,
    ResponseError,
)


BACKOFF_FACTOR = 2
BACKOFF_STATUSES = set(('403', '408', '429', '500', '502', '503', '504'))

ContentProducer = Callable[[], AsyncGenerator[bytes, None]]
ReadableContent = Union[bytes, ContentProducer]


class Network(object):

    def __init__(self, storage: OAuth2Storage, timeout: int) -> None:
        self._storage = storage
        self._timeout = timeout
        self._backoff_level = 0
        self._session = None
        self._oauth = None
        self._raii = None

    async def __aenter__(self) -> 'Network':
        async with contextlib.AsyncExitStack() as stack:
            self._session = await stack.enter_async_context(
                aiohttp.ClientSession())
            self._oauth = await stack.enter_async_context(
                OAuth2Manager(self._session, self._storage))
            self._raii = stack.pop_all()

        return self

    async def __aexit__(self, type_, value, traceback) -> bool:
        await self._raii.aclose()
        self._backoff_level = 0
        self._session = None
        self._oauth = None
        self._raii = None

    async def fetch(self,
        method: str,
        url: str,
        args: Dict[str, Any] = None,
        headers: Dict[str, str] = None,
        body: ReadableContent = None,
    ) -> 'JSONResponse':
        while True:
            kwargs = await self._prepare_kwargs(method, url, args, headers,
                                                body)

            try:
                response = await self._request_loop(kwargs, True)
            except aiohttp.ClientConnectionError as e:
                continue

            try:
                return await to_json_response(response)
            except aiohttp.ClientConnectionError as e:
                # The server recived the request, but reading body failed.
                # We are in a broken state, just let client to handle it.
                raise NetworkError() from e

    async def upload(self,
        method: str,
        url: str,
        args: Dict[str, Any] = None,
        headers: Dict[str, str] = None,
        body: ReadableContent = None,
    ) -> 'JSONResponse':
        kwargs = await self._prepare_kwargs(method, url, args, headers, body)
        kwargs['timeout'] = 0.0

        # NOTE Unlike download, upload cannot set timeout to the whole method.
        # Instead we should set timeout on body callback.
        try:
            response = await self._request_loop(kwargs, False)
        except aiohttp.ClientConnectionError as e:
            raise NetworkError() from e

        return await to_json_response(response)

    async def download(self,
        method: str,
        url: str,
        args: Dict[str, Any] = None,
        headers: Dict[str, str] = None,
        body: ReadableContent = None,
    ) -> 'StreamResponse':
        while True:
            kwargs = await self._prepare_kwargs(method, url, args, headers,
                                                body)
            kwargs['timeout'] = 0.0

            try:
                response = await self._request_loop(kwargs, True)
            except aiohttp.ClientConnectionError:
                continue

            return StreamResponse(response, self._timeout)

    async def _request_loop(self,
        kwargs: Dict[str, Any],
        use_timeout: bool,
    ) -> aiohttp.ClientResponse:
        while True:
            await self._wait_backoff()

            f = self._session.request(**kwargs)
            if use_timeout:
                f = asyncio.wait_for(f, self._timeout)
            try:
                response = await f
            except aiohttp.ClientConnectionError:
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

            try:
                json_ = await response.json()
            except aiohttp.ContentTypeError as e:
                text = await response.text()
                EXCEPTION('wcpan.drive.google', e) << text
                raise

            self._raiseError(status, response, json_)

    async def _prepare_kwargs(self,
        method: str,
        url: str,
        args: Optional[Dict[str, Any]],
        headers: Optional[Dict[str, str]],
        body: Optional[ReadableContent],
    ) -> Dict[str, Any]:
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
        headers: Optional[Dict[str, str]],
    ) -> Dict[str, str]:
        if headers is None:
            h = {}
        else:
            h = {k: v if isinstance(v, (bytes, str)) or v is None else str(v)
                 for k, v in headers.items()}
        await self._update_token_header(h)
        return h

    async def _update_token_header(self, headers: Dict[str, str]) -> None:
        token = await self._oauth.get_access_token()
        headers['Authorization'] = f'Bearer {token}'

    async def _check_status(self,
        status: str,
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
        s_delay = min(self._timeout, s_delay)
        DEBUG('wcpan.drive.google') << 'backoff for' << s_delay
        await asyncio.sleep(s_delay)

    def _raiseError(self,
        status: str,
        response: aiohttp.ClientResponse,
        json_: Dict[str, Any],
    ) -> None:
        if status == '403':
            firstError = json_['error']['errors'][0]
            reason = firstError['reason']
            if reason == 'cannotDownloadAbusiveFile':
                raise DownloadAbusiveFileError(firstError['message'])
            if reason == 'invalidAbuseAcknowledgment':
                raise InvalidAbuseFlagError(firstError['message'])
        raise ResponseError(status, response, json_)


class Request(object):

    def __init__(self, request: aiohttp.RequestInfo) -> None:
        self._request = request

    @property
    def uri(self) -> str:
        return self._request.url

    @property
    def method(self) -> str:
        return self._request.method

    @property
    def headers(self) -> Dict[str, str]:
        return self._request.headers


class Response(object):

    def __init__(self, response: aiohttp.ClientResponse) -> None:
        self._response = response
        self._status = str(response.status)
        self._request = Request(response.request_info)

    @property
    def status(self):
        return self._status

    def get_header(self, key: str) -> str:
        h = self._response.headers.getall(key)
        return None if not h else h[0]


class JSONResponse(Response):

    def __init__(self,
        response: aiohttp.ClientResponse,
        json_: Dict[str, Any],
    ) -> None:
        super().__init__(response)
        self._json = json_

    @property
    def json(self) -> Dict[str, Any]:
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


class Status(enum.Enum):

    OK = enum.auto()
    REFRESH = enum.auto()
    BACKOFF = enum.auto()
    UNKNOWN = enum.auto()


async def backoff_needed(
    status: str,
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
    qs: Dict[str, Any],
) -> Generator[Tuple[str, str], None, None]:
    for key, value in qs.items():
        if isinstance(value, bool):
            value = 'true' if value else 'false'
        elif isinstance(value, (int, float)):
            value = str(value)
        elif not isinstance(value, str):
            raise ValueError('unknown type in query string')
        yield key, value
