import datetime as dt
import hashlib
import json
import os
import os.path as op
import re
from typing import Any, BinaryIO, Dict, Text

import yaml


ISO_PATTERN = r'^(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2}):(\d{2})(.(\d{3,6}))?(Z|(([+\-])(\d{2}):(\d{2})))$'
CHUNK_SIZE = 64 * 1024
FOLDER_MIME_TYPE = 'application/vnd.google-apps.folder'


class GoogleDriveError(Exception):
    pass


class InvalidDateTimeError(GoogleDriveError):

    def __init__(self, iso: Text) -> None:
        self._iso = iso

    def __str__(self) -> Text:
        return 'invalid ISO date: ' + self._iso


class Settings(object):

    def __init__(self, path: Text) -> None:
        self._path = path
        self._data = {
            'version': 1,
            'save_credentials': False,
            'client_config_backend': 'settings',
            'nodes_database_file': ':memory:',
        }
        self._initialize()

    def _initialize(self) -> None:
        if not self._path:
            return

        # default values for file
        self._data['version'] = 1
        self._data['client_config_backend'] = 'file'
        self._data['client_config_file'] = op.join(self._path, 'client_secret.json')
        self._data['get_refresh_token'] = True
        self._data['save_credentials'] = True
        self._data['save_credentials_backend'] = 'file'
        self._data['save_credentials_file'] = op.join(self._path, 'oauth_token.yaml')
        self._data['nodes_database_file'] = op.join(self._path, 'nodes.db')

        os.makedirs(self._path, exist_ok=True)
        path = op.join(self._path, 'settings.yaml')
        if not op.exists(path):
            rv = yaml.dump(self._data, default_flow_style=False)
            with open(path, 'w') as fout:
                fout.write(rv)
        else:
            with open(path, 'r') as fin:
                rv = yaml.load(fin)
            self._data.update(rv)

    def __getitem__(self, key: Text) -> Any:
        return self._data[key]

    async def load_oauth2_info(self) -> Dict[Text, Any]:
        if 'client_config_file' not in self._data:
            raise ValueError('`client_config_file` not found')

        # load API key
        with open(self._data['client_config_file'], 'r') as fin:
            client = json.load(fin)
        if 'installed' not in client:
            raise ValueError('credential should be an installed application')
        client = client['installed']
        redirect_uri = client['redirect_uris'][0]
        client_id = client['client_id']
        client_secret = client['client_secret']

        # load refresh token
        token_path = self._data['save_credentials_file']
        if not op.isfile(token_path):
            access_token = None
            refresh_token = None
        else:
            with open(token_path, 'r') as fin:
                token = yaml.safe_load(fin)
            if token.get('version', 0) != 1:
                raise ValueError('wrong token file')
            access_token = token['access_token']
            refresh_token = token['refresh_token']

        return {
            'client_id': client_id,
            'client_secret': client_secret,
            'redirect_uri': redirect_uri,
            'access_token': access_token,
            'refresh_token': refresh_token,
        }

    async def save_oauth2_info(self,
            access_token: Text,
            refresh_token: Text,
        ) -> None:
        token = {
            'version': 1,
            'access_token': access_token,
            'refresh_token': refresh_token,
        }
        token_path = self._data['save_credentials_file']

        # save refresh token
        rv = yaml.dump(token, default_flow_style=False)
        with open(token_path, 'w') as fout:
            fout.write(rv)


def from_isoformat(iso_datetime: Text) -> dt.datetime:
    rv = re.match(ISO_PATTERN, iso_datetime)
    if not rv:
        raise InvalidDateTimeError(iso_datetime)

    year = int(rv.group(1), 10)
    month = int(rv.group(2), 10)
    day = int(rv.group(3), 10)
    hour = int(rv.group(4), 10)
    minute = int(rv.group(5), 10)
    second = int(rv.group(6), 10)
    if rv.group(8):
        microsecond = rv.group(8).ljust(6, '0')
        microsecond = int(microsecond, 10)
    else:
        microsecond = 0
    tz = rv.group(9)
    if tz == 'Z':
        tz = dt.timezone.utc
    else:
        f = rv.group(11)
        h = int(rv.group(12), 10)
        m = int(rv.group(13), 10)
        tz = dt.timedelta(hours=h, minutes=m)
        if f == '-':
            tz = -tz
        tz = dt.timezone(tz)

    rv = dt.datetime(year, month, day, hour, minute, second, microsecond, tz)
    return rv


def stream_md5sum(input_stream: BinaryIO) -> Text:
    hasher = hashlib.md5()
    while True:
        chunk = input_stream.read(CHUNK_SIZE)
        if not chunk:
            break
        hasher.update(chunk)
    return hasher.hexdigest()
