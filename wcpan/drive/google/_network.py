from collections.abc import Iterable, AsyncIterable, AsyncIterator
from contextlib import asynccontextmanager
from logging import getLogger
from typing import Any
import asyncio
import math
import random

from aiohttp import ClientSession, ClientResponse
from wcpan.drive.core.exceptions import UnauthorizedError

from ._oauth import OAuth2Manager
from .exceptions import DownloadAbusiveFileError, InvalidAbuseFlagError


_BACKOFF_FACTOR = 2
_BACKOFF_MAX_TIMEOUT = 60

type QueryDict = dict[str, int | bool | str]
type ReadableContent = bytes | AsyncIterable[bytes]


@asynccontextmanager
async def create_network(oauth: OAuth2Manager):
    async with ClientSession() as session:
        yield Network(session, oauth)


class Network:
    """
    Handles
    1. OAuth2.0 token refreshing.
    2. Exponential backoff on error.
    """

    def __init__(self, session: ClientSession, oauth: OAuth2Manager) -> None:
        self._oauth = oauth
        self._session = session
        self._backoff = BackoffController()

    async def accept_oauth_code(self, code: str) -> None:
        await self._oauth.set_authenticated_token(self._session, code)

    @asynccontextmanager
    async def fetch(
        self,
        method: str,
        url: str,
        *,
        query: QueryDict | None = None,
        headers: dict[str, str] | None = None,
        body: ReadableContent | None = None,
        timeout: bool = True,
    ) -> AsyncIterator[ClientResponse]:
        kwargs = await self._prepare_kwargs(method, url, query, headers, body)

        # NOTE Upload or download can take long time.
        # The actual timeout will be controled by the caller.
        if not timeout:
            kwargs["timeout"] = None

        async with self._retry_fetch(kwargs) as request:
            yield request

    @asynccontextmanager
    async def _retry_fetch(
        self, kwargs: dict[str, Any]
    ) -> AsyncIterator[ClientResponse]:
        """
        Send request and retries when following happens:
        1. Need to refresh access token.
        2. Rate limit exceeed.
        Other cases should just raise exceptions.
        """
        while True:
            await self._backoff.wait()

            async with self._session.request(**kwargs) as response:
                # 1xx, 2xx, 3xx
                if response.status < 400:
                    # Successful request should decrease backoff level.
                    self._backoff.decrease()
                    yield response
                    # NOTE: Do not forget to exit the loop.
                    return

                # 5xx server error, nothing we can do.
                if response.status >= 500:
                    # The server is unstable, increase backoff level.
                    self._backoff.increase()
                    response.raise_for_status()

                # 401 usually means access token expired.
                if response.status == 401:
                    await self._refresh_access_token(kwargs["headers"])
                    continue

                # 403 can be rate limit error.
                if response.status == 403:
                    await self._handle_403(response)
                    # If the handler does not raise exception,
                    # it should be rate limit error.
                    # Increase backoff level and try again.
                    self._backoff.increase()
                    continue

                # Other 4xx errors are general HTTP errors.
                await self._handle_4xx(response)
                # Just in case the loop does not stop.
                return

    async def _refresh_access_token(self, headers: dict[str, str]):
        try:
            await self._oauth.renew_token(self._session)
        except UnauthorizedError:
            raise
        except Exception as e:
            raise UnauthorizedError() from e
        await self._update_token_header(headers)

    async def _handle_403(self, response: ClientResponse):
        # Not all 403 errors are rate limit error.

        data = await response.json()
        if not data:
            # Undocumented behavior, probably a server problem.
            getLogger(__name__).error("403 with empty error message")
            response.raise_for_status()

        # FIXME: May have multiple errors.
        firstError = data["error"]["errors"][0]
        domain = firstError["domain"]
        reason = firstError["reason"]
        message = firstError["message"]
        if domain == "usageLimits":
            getLogger(__name__).warning("hit api rate limit")
            return
        if reason == "cannotDownloadAbusiveFile":
            raise DownloadAbusiveFileError(message)
        if reason == "invalidAbuseAcknowledgment":
            raise InvalidAbuseFlagError(message)

        getLogger(__name__).error(f"{data}")
        response.raise_for_status()

    async def _handle_4xx(self, response: ClientResponse):
        # 408 can be gateway timeout, which payload is not always JSON.
        if response.status != 408:
            data = await response.json()
            getLogger(__name__).error(f"{data}")

        response.raise_for_status()

    async def _prepare_kwargs(
        self,
        method: str,
        url: str,
        query: QueryDict | None,
        headers: dict[str, str] | None,
        body: ReadableContent | None,
    ) -> dict[str, Any]:
        kwargs: dict[str, Any] = {
            "method": method,
            "url": url,
            "headers": await self._prepare_headers(headers),
        }
        if query is not None:
            kwargs["params"] = list(_normalize_query_string(query))
        if body is not None:
            kwargs["data"] = body
        return kwargs

    async def _prepare_headers(
        self,
        headers: dict[str, str] | None,
    ) -> dict[str, str]:
        if headers is None:
            headers = {}
        await self._update_token_header(headers)
        return headers

    async def _update_token_header(self, headers: dict[str, str]) -> None:
        token = await self._oauth.safe_get_access_token()
        if not token:
            raise UnauthorizedError()
        headers["Authorization"] = f"Bearer {token}"


class BackoffController:
    def __init__(self) -> None:
        self._level = 0

    async def wait(self) -> None:
        if self._level <= 0:
            return
        seed = random.random()
        power = 2**self._level
        s_delay = math.floor(seed * power * _BACKOFF_FACTOR)
        s_delay = min(_BACKOFF_MAX_TIMEOUT, s_delay)
        getLogger(__name__).debug(f"backoff for {s_delay} seconds")
        await asyncio.sleep(s_delay)

    def increase(self) -> None:
        self._level = min(self._level + 2, 10)

    def decrease(self) -> None:
        self._level = max(self._level - 1, 0)


def _normalize_query_string(
    qs: QueryDict,
) -> Iterable[tuple[str, str]]:
    for key, value in qs.items():
        if isinstance(value, bool):
            value = "true" if value else "false"
        elif isinstance(value, int):
            value = str(value)
        yield key, value
