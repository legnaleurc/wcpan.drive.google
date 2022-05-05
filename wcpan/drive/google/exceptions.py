from typing import Any

import aiohttp
from wcpan.drive.core.exceptions import DriveError, DownloadError


class AuthenticationError(DriveError):
    pass


class CredentialFileError(DriveError):
    pass


class TokenFileError(DriveError):
    pass


class NetworkError(DriveError):
    pass


class ResponseError(DriveError):

    def __init__(self,
        status: str,
        response: aiohttp.ClientResponse,
        json_: dict[str, Any],
    ) -> None:
        self._status = status
        self._response = response
        self._message = f'{self.status} {self._response.reason} - {json_}'
        self._json = json_

    def __str__(self) -> str:
        return self._message

    @property
    def status(self) -> str:
        return self._status

    @property
    def json(self) -> Any:
        return self._json


class DownloadAbusiveFileError(DownloadError):
    pass


class InvalidAbuseFlagError(DownloadError):
    pass


class InvalidRangeError(DownloadError):
    pass
