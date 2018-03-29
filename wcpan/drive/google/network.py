import asyncio
import json
import math
import random
import urllib.parse as up

import aiohttp
from wcpan.logger import DEBUG, EXCEPTION, INFO, WARNING

from .util import GoogleDriveError


BACKOFF_FACTOR = 2
BACKOFF_STATUSES = ('403', '500', '502', '503', '504')


class Network(object):

    def __init__(self):
        self._access_token = None
        self._backoff_level = 0
        self._session = None

    async def __aenter__(self):
        self._session = aiohttp.ClientSession()
        await self._session.__aenter__()
        return self

    async def __aexit__(self, exc_type, exc, tb):
        await self._session.__aexit__(exc_type, exc, tb)

    def set_access_token(self, token):
        self._access_token = token

    async def fetch(self, method, path, args=None, headers=None, body=None,
                    raise_internal_error=False):
        while True:
            await self._maybe_backoff()
            try:
                rv = await self._do_request(method, path, args, headers, body,
                                            raise_internal_error)
                return rv
            except NetworkError as e:
                if e.status != '599':
                    raise
                if e._response.raise_internal_error:
                    raise
                WARNING('wcpan.drive.google') << str(e)

    async def _do_request(self, method, path, args, headers, body,
                          raise_internal_error):
        headers = self._prepare_headers(headers)

        kwargs = {
            'method': method,
            'url': url,
            'headers': headers,
        }
        if args is not None:
            kwargs['params'] = list(normalize_query_string(args))
        if body is not None:
            kwargs['data'] = body if not callable(body) else body()
        if raise_internal_error:
            # do not raise timeout from client
            kwargs['timeout'] = 0

        response = await self._session.request(**kwargs)

        rv = Response(response, raise_internal_error)
        rv = await self._handle_status(rv)
        return rv

    def _prepare_headers(self, headers):
        h = {
            'Authorization': 'Bearer {0}'.format(self._access_token),
        }
        if headers is not None:
            h.update(headers)
        h = {k: v if isinstance(v, (bytes, str)) or v is None else str(v)
             for k, v in h.items()}
        return h

    async def _handle_status(self, response):
        backoff = await backoff_needed(response)
        if backoff:
            self._increase_backoff_level()
        else:
            self._decrease_backoff_level()

        # normal response
        if response.status[0] in ('1', '2', '3'):
            return response

        # otherwise it is an error
        json_ = await response.json()
        raise NetworkError(response, json_, not backoff)

    def _increase_backoff_level(self):
        self._backoff_level = min(self._backoff_level + 2, 10)

    def _decrease_backoff_level(self):
        self._backoff_level = max(self._backoff_level - 1, 0)

    async def _maybe_backoff(self):
        if self._backoff_level <= 0:
            return
        seed = random.random()
        power = 2 ** self._backoff_level
        s_delay = math.floor(seed * power * BACKOFF_FACTOR)
        s_delay = min(100, s_delay)
        DEBUG('wcpan.drive.google') << 'backoff for' << s_delay
        await asyncio.sleep(s_delay)


class Request(object):

    def __init__(self, request):
        self._request = request

    @property
    def uri(self):
        return self._request.url

    @property
    def method(self):
        return self._request.method

    @property
    def headers(self):
        return self._request.headers


class Response(object):

    def __init__(self, response, raise_internal_error):
        self._response = response
        self._raise_internal_error = raise_internal_error
        self._request = Request(response.request_info)
        self._status = str(response.status)
        self._parsed_json = False
        self._json = None

    @property
    def status(self):
        return self._status

    @property
    def reason(self):
        return self._response.reason

    async def json(self):
        if self._parsed_json:
            return self._json

        try:
            rv = await self._response.json()
        except aiohttp.ContentTypeError as e:
            EXCEPTION('wcpan.drive.google') << rv
            rv = None
        except ValueError as e:
            EXCEPTION('wcpan.drive.google') << rv
            rv = None

        self._json = rv
        self._parsed_json = True

        return self._json

    def chunks(self):
        return self._response.content.iter_any()

    @property
    def request(self):
        return self._request

    @property
    def raise_internal_error(self):
        return self._raise_internal_error

    def get_header(self, key):
        h = self._response.headers.getall(key)
        return None if not h else h[0]


class NetworkError(GoogleDriveError):

    def __init__(self, response, json_, fatal):
        self._response = response
        self._message = '{0} {1} - {2}'.format(self.status,
                                               self._response.reason,
                                               json_)
        self._fatal = fatal

    def __str__(self):
        return self._message

    @property
    def status(self):
        return self._response.status

    @property
    def fatal(self):
        return self._fatal

    @property
    def json(self):
        return self._json


async def backoff_needed(response):
    if response.status not in BACKOFF_STATUSES:
        return False

    # if it is not a rate limit error, it could be handled immediately
    if response.status == '403':
        msg = await response.json()
        if not msg:
            WARNING('wcpan.drive.google') << '403 with empty error message'
            # probably server problem, backoff for safety
            return True
        domain = msg['error']['errors'][0]['domain']
        if domain != 'usageLimits':
            return False
        INFO('wcpan.drive.google') << msg['error']['message']

    return True


def normalize_query_string(qs):
    for key, value in qs.items():
        if isinstance(value, bool):
            value = 'true' if value else 'false'
        elif isinstance(value, (int, float)):
            value = str(value)
        elif not isinstance(value, str):
            raise ValueError('unknown type in query string')
        yield key, value
