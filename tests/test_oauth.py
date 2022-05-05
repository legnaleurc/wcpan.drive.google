from typing import Any
from unittest.mock import MagicMock
import contextlib
import json
import pathlib
import tempfile
import unittest

import yaml

from wcpan.drive.google.util import OAuth2Storage, OAuth2Manager
from wcpan.drive.google.exceptions import CredentialFileError, TokenFileError

from .http_client import FakeClient


class TestOAuth2Storage(unittest.TestCase):

    def setUp(self):
        with contextlib.ExitStack() as stack:
            config_path = stack.enter_context(tempfile.TemporaryDirectory())
            data_path = stack.enter_context(tempfile.TemporaryDirectory())
            self._config_path = pathlib.Path(config_path)
            self._data_path = pathlib.Path(data_path)
            self._storage = OAuth2Storage(
                self._config_path,
                self._data_path,
            )
            self._raii = stack.pop_all()

    def tearDown(self):
        self._raii.close()
        self._storage = None
        self._config_path = None
        self._data_path = None

    def testLoadError1(self):
        with self.assertRaises(FileNotFoundError):
            self._storage.load_oauth2_config()

    def testLoadError2(self):
        write_config(self._config_path, {})
        with self.assertRaises(CredentialFileError):
            self._storage.load_oauth2_config()

    def testLoadError3(self):
        write_token(self._data_path, {
            'version': -1,
        })
        with self.assertRaises(TokenFileError):
            self._storage.load_oauth2_token()

    def testLoadConfig(self):
        write_default_config(self._config_path)
        rv = self._storage.load_oauth2_config()
        self.assertEqual(rv['client_id'], '__ID__')
        self.assertEqual(rv['client_secret'], '__SECRET__')
        self.assertEqual(rv['redirect_uri'], '__REDIRECT_URI__')
        self.assertEqual(rv['token_uri'], '__TOKEN_URI__')
        self.assertEqual(rv['auth_uri'], '__AUTH_URI__')

    def testLoadToken(self):
        write_token(self._data_path, {
            'version': 1,
            'access_token': '__ACCESS__',
            'refresh_token': '__REFRESH__',
        })
        rv = self._storage.load_oauth2_token()
        self.assertEqual(rv['access_token'], '__ACCESS__')
        self.assertEqual(rv['refresh_token'], '__REFRESH__')

    def testSaveToken(self):
        self._storage.save_oauth2_token(
            access_token='__ACCESS__',
            refresh_token='__REFRESH__',
        )
        rv = read_token(self._data_path)
        self.assertEqual(rv['version'], 1)
        self.assertEqual(rv['access_token'], '__ACCESS__')
        self.assertEqual(rv['refresh_token'], '__REFRESH__')


class TestOAuth2Authenticator(unittest.IsolatedAsyncioTestCase):

    def setUp(self) -> None:
        self._storage = MagicMock()
        self._storage.load_oauth2_config.return_value = {
            'redirect_uri': '__REDIRECT_URI__',
            'auth_uri': '__AUTH_URI__',
            'token_uri': '__TOKEN_URI__',
            'client_id': '__ID__',
            'client_secret': '__SECRET__',
        }
        self._storage.load_oauth2_token.return_value = {
            'access_token': '__ACCESS__',
            'refresh_token': '__REFRESH__',
        }
        self._oauth = OAuth2Manager(self._storage)

    def tearDown(self) -> None:
        self._oauth = None
        self._storage = None

    async def testAuthorizeStatus(self):
        storage = MagicMock()
        storage.load_oauth2_config.return_value = {}
        storage.load_oauth2_token.return_value = {}
        oauth = OAuth2Manager(storage)
        self.assertIsNone(oauth.access_token)
        self.assertIsNone(oauth.refresh_token)

        storage = MagicMock()
        storage.load_oauth2_config.return_value = {}
        storage.load_oauth2_token.return_value = {
            'access_token': '__ACCESS__',
            'refresh_token': '__REFRESH__',
        }
        oauth = OAuth2Manager(storage)
        self.assertEqual(oauth.access_token, '__ACCESS__')
        self.assertEqual(oauth.refresh_token, '__REFRESH__')

    async def testGetOAuthUrl(self):
        url = self._oauth.build_authorization_url()
        self.assertEqual(url, '__AUTH_URI__?redirect_uri=__REDIRECT_URI__&client_id=__ID__&response_type=code&access_type=offline&scope=https%3A%2F%2Fwww.googleapis.com%2Fauth%2Fdrive')

    async def testAcceptAnswer(self):
        answer = 'http://localhost/?code=__CODE__&scope=__SCOPE__'

        session = FakeClient()
        session.add_json({
            'access_token': '__NEW_ACCESS__',
            'refresh_token': '__NEW_REFRESH__',
        })

        await self._oauth.set_authenticated_token(session, answer)
        self.assertEqual(self._oauth.access_token, '__NEW_ACCESS__')
        self.assertEqual(self._oauth.refresh_token, '__NEW_REFRESH__')
        self._storage.save_oauth2_token.assert_called()

        called_list = session.get_request_sequence()
        self.assertEqual(called_list, [
            {
                'method': 'POST',
                'url': '__TOKEN_URI__',
                'headers': {
                    'Content-Type': 'application/x-www-form-urlencoded',
                },
                'data': [
                    ('redirect_uri', '__REDIRECT_URI__'),
                    ('code', '__CODE__'),
                    ('client_id', '__ID__'),
                    ('client_secret', '__SECRET__'),
                    ('grant_type', 'authorization_code'),
                ],
            },
        ])

    async def testRefresh(self):
        session = FakeClient()
        session.add_json({
            'access_token': '__NEW_ACCESS__',
        })

        await self._oauth.renew_token(session)
        self.assertEqual(self._oauth.access_token, '__NEW_ACCESS__')
        self.assertEqual(self._oauth.refresh_token, '__REFRESH__')
        self._storage.save_oauth2_token.assert_called()

        called_list = session.get_request_sequence()
        self.assertEqual(called_list, [
            {
                'method': 'POST',
                'url': '__TOKEN_URI__',
                'headers': {
                    'Content-Type': 'application/x-www-form-urlencoded',
                },
                'data': [
                    ('client_id', '__ID__'),
                    ('client_secret', '__SECRET__'),
                    ('refresh_token', '__REFRESH__'),
                    ('grant_type', 'refresh_token'),
                ],
            },
        ])


def write_config(config_path: pathlib.Path, dict_: dict[str, Any]) -> None:
    file_path = config_path / 'client_secret.json'
    with file_path.open('w') as fout:
        json.dump(dict_, fout)


def write_default_config(config_path: pathlib.Path) -> None:
    write_config(config_path, {
        'web': {
            'redirect_uris': [
                '__REDIRECT_URI__',
            ],
            'auth_uri': '__AUTH_URI__',
            'token_uri': '__TOKEN_URI__',
            'client_id': '__ID__',
            'client_secret': '__SECRET__',
        },
    })


def write_token(data_path: pathlib.Path, dict_: dict[str, Any]) -> None:
    file_path = data_path / 'oauth_token.yaml'
    with file_path.open('w') as fout:
        yaml.dump(dict_, fout)


def read_token(data_path: pathlib.Path) -> dict[str, Any]:
    file_path = data_path / 'oauth_token.yaml'
    with file_path.open('r') as fin:
        rv = yaml.safe_load(fin)
    return rv
