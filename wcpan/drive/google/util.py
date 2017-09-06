import datetime as dt
import functools as ft
import hashlib
import json
import os
import os.path as op
import re
import urllib.parse as up

from tornado import auth as ta
import yaml


ISO_PATTERN = r'^(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2}):(\d{2})(.(\d{3,6}))?(Z|(([+\-])(\d{2}):(\d{2})))$'
CHUNK_SIZE = 64 * 1024
FOLDER_MIME_TYPE = 'application/vnd.google-apps.folder'


class GoogleDriveError(Exception):
    pass


class InvalidDateTimeError(GoogleDriveError):

    def __init__(self, iso):
        self._iso = iso

    def __str__(self):
        return 'invalid ISO date: ' + self._iso


class Settings(object):

    def __init__(self, path):
        self._path = path
        self._data = {
            'version': 1,
            'save_credentials': False,
            'client_config_backend': 'settings',
            'nodes_database_file': ':memory:',
        }
        self._initialize()

    def _initialize(self):
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

    def __getitem__(self, key):
        return self._data[key]


class CommandLineOAuth2Handler(ta.GoogleOAuth2Mixin):

    def __init__(self, cfg):
        super(CommandLineOAuth2Handler, self).__init__()

        self._load(cfg)
        self._code = ''
        self._settings = {
            'google_oauth': {
                'key': self._client_id,
                'secret': self._client_secret,
            },
        }

    async def authorize(self):
        if self._access_token is None:
            await self._get_access_token()

    async def refresh_access_token(self):
        curl = self.get_auth_http_client()
        body = up.urlencode({
            'client_id': self._client_id,
            'client_secret': self._client_secret,
            'refresh_token': self._refresh_token,
            'grant_type': 'refresh_token',
        })
        headers = {
            'Content-Type': 'application/x-www-form-urlencoded',
        }
        response = await curl.fetch(self._OAUTH_ACCESS_TOKEN_URL,
                                    method='POST', headers=headers, body=body)
        if response.error:
            raise ValueError('Google OAuth 2.0 error: {0}'.format(response))
        token = json.loads(response.body)
        self._save_token(token)

    async def _get_access_token(self):
        await self.authorize_redirect(redirect_uri=self._redirect_uri,
                                      scope=['https://www.googleapis.com/auth/drive'],
                                      client_id=self._client_id)
        token = await self.get_authenticated_user(self._redirect_uri, code=self._code)
        self._code = ''
        self._save_token(token)

    @property
    def access_token(self):
        assert self._access_token is not None
        return self._access_token

    def redirect(self, url):
        print(url)
        self._code = input()

    @property
    def settings(self):
        return self._settings

    def _load(self, cfg):
        if 'client_config_file' not in cfg:
            raise ValueError('`client_config_file` not found')

        # load API key
        with open(cfg['client_config_file'], 'r') as fin:
            client = json.load(fin)
        if 'installed' not in client:
            raise ValueError('credential should be an installed application')
        client = client['installed']
        self._redirect_uri = client['redirect_uris'][0]
        self._client_id = client['client_id']
        self._client_secret = client['client_secret']

        # load refresh token
        self._token_path = cfg['save_credentials_file']
        if not op.isfile(self._token_path):
            self._access_token = None
            self._refresh_token = None
            return
        with open(self._token_path, 'r') as fin:
            token = yaml.safe_load(fin)
        if token.get('version', 0) != 1:
            raise ValueError('wrong token file')
        self._access_token = token['access_token']
        self._refresh_token = token['refresh_token']

    def _save_token(self, token):
        self._access_token = token['access_token']
        if 'refresh_token' in token:
            self._refresh_token = token['refresh_token']
        token = {
            'version': 1,
            'access_token': self._access_token,
            'refresh_token': self._refresh_token,
        }

        # save refresh token
        rv = yaml.dump(token, default_flow_style=False)
        with open(self._token_path, 'w') as fout:
            fout.write(rv)


def from_isoformat(iso_datetime):
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


def stream_md5sum(input_stream):
    hasher = hashlib.md5()
    while True:
        chunk = input_stream.read(CHUNK_SIZE)
        if not chunk:
            break
        hasher.update(chunk)
    return hasher.hexdigest()
