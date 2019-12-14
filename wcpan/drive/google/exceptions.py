from typing import Dict, Any
import aiohttp

from wcpan.drive.core.exceptions import DriveError, DownloadError


class AuthenticationError(DriveError):
    pass


class CredentialFileError(DriveError):

    @staticmethod
    def _get_dummy_config_format() -> str:
        import json
        dict_ = {
            'installed': {
                'redirect_uris': [
                    '__URI__',
                ],
                'client_id': '__ID__',
                'client_secret': '__SECRET__',
            },
        }
        return json.dumps(dict_, ensure_ascii=False, indent=2)

    def __init__(self) -> None:
        super().__init__(self, (
            'invalid config file format, correct example:\n'
            f'{CredentialFileError._get_dummy_config_format()}'
        ))


class TokenFileError(DriveError):
    pass


class NetworkError(DriveError):
    pass


class ResponseError(DriveError):

    def __init__(self,
        status: str,
        response: aiohttp.ClientResponse,
        json_: Dict[str, Any],
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
