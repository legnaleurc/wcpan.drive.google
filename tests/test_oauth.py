from typing import Dict, Any
from unittest.mock import AsyncMock, patch
import contextlib
import json
import pathlib
import tempfile
import unittest

import yaml

from wcpan.drive.google.util import OAuth2Storage, OAuth2CommandLineAuthenticator
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
            self._storage.load_oauth2_info()

    def testLoadError2(self):
        write_config(self._config_path, {})
        with self.assertRaises(CredentialFileError):
            self._storage.load_oauth2_info()

    def testLoadEmptyToken(self):
        write_default_config(self._config_path)
        rv = self._storage.load_oauth2_info()
        self.assertEqual(rv['client_id'], '__ID__')
        self.assertEqual(rv['client_secret'], '__SECRET__')
        self.assertEqual(rv['redirect_uri'], '__URI__')
        self.assertIsNone(rv['access_token'])
        self.assertIsNone(rv['refresh_token'])

    def testLoadError3(self):
        write_default_config(self._config_path)
        write_token(self._data_path, {
            'version': -1,
        })
        with self.assertRaises(TokenFileError):
            self._storage.load_oauth2_info()

    def testLoadWithToken(self):
        write_default_config(self._config_path)
        write_token(self._data_path, {
            'version': 1,
            'access_token': '__ACCESS__',
            'refresh_token': '__REFRESH__',
        })
        rv = self._storage.load_oauth2_info()
        self.assertEqual(rv['client_id'], '__ID__')
        self.assertEqual(rv['client_secret'], '__SECRET__')
        self.assertEqual(rv['redirect_uri'], '__URI__')
        self.assertEqual(rv['access_token'], '__ACCESS__')
        self.assertEqual(rv['refresh_token'], '__REFRESH__')

    def testSave(self):
        self._storage.save_oauth2_info(
            access_token='__ACCESS__',
            refresh_token='__REFRESH__',
        )
        rv = read_token(self._data_path)
        self.assertEqual(rv['version'], 1)
        self.assertEqual(rv['access_token'], '__ACCESS__')
        self.assertEqual(rv['refresh_token'], '__REFRESH__')


class TestOAuth2Authenticator(unittest.IsolatedAsyncioTestCase):

    async def testFirstRun(self):
        class_ = OAuth2CommandLineAuthenticator

        session = FakeClient()
        with patch.object(class_, 'redirect') as fake_redirect:
            fake_redirect.return_value = '__PASTED_TOKEN__'

            session.add_json({
                'access_token': '__ACCESS__',
                'refresh_token': '__REFRESH__',
            })
            async with class_(
                session,
                '__ID__',
                '__SECRET__',
                '__URI__',
                None,
                None,
            ) as oauth2:
                self.assertEqual(oauth2.access_token, '__ACCESS__')
                self.assertEqual(oauth2.refresh_token, '__REFRESH__')

            called_list = session.get_request_sequence()
            self.assertEqual(called_list, [
                {
                    'method': 'POST',
                    'url': 'https://accounts.google.com/o/oauth2/token',
                    'headers': {
                        'Content-Type': 'application/x-www-form-urlencoded',
                    },
                    'data': [
                        ('redirect_uri', '__URI__'),
                        ('code', '__PASTED_TOKEN__'),
                        ('client_id', '__ID__'),
                        ('client_secret', '__SECRET__'),
                        ('grant_type', 'authorization_code'),
                    ],
                },
            ])

    async def testRefresh(self):
        class_ = OAuth2CommandLineAuthenticator

        session = FakeClient()
        with patch.object(class_, 'redirect') as fake_redirect:
            fake_redirect.return_value = '__PASTED_TOKEN__'

            session.add_json({
                'access_token': '__ACCESS__',
                'refresh_token': '__REFRESH__',
            })
            async with class_(
                session,
                '__ID__',
                '__SECRET__',
                '__URI__',
                None,
                None,
            ) as oauth2:
                session.reset()

                session.add_json({
                    'access_token': '__NEW_ACCESS__',
                    'refresh_token': '__NEW_REFRESH__',
                })
                await oauth2.refresh()
                self.assertEqual(oauth2.access_token, '__NEW_ACCESS__')
                self.assertEqual(oauth2.refresh_token, '__NEW_REFRESH__')


def write_config(config_path: pathlib.Path, dict_: Dict[str, Any]) -> None:
    file_path = config_path / 'client_secret.json'
    with file_path.open('w') as fout:
        json.dump(dict_, fout)


def write_default_config(config_path: pathlib.Path) -> None:
    write_config(config_path, {
        'installed': {
            'redirect_uris': [
                '__URI__',
            ],
            'client_id': '__ID__',
            'client_secret': '__SECRET__',
        },
    })


def write_token(data_path: pathlib.Path, dict_: Dict[str, Any]) -> None:
    file_path = data_path / 'oauth_token.yaml'
    with file_path.open('w') as fout:
        yaml.dump(dict_, fout)


def read_token(data_path: pathlib.Path) -> Dict[str, Any]:
    file_path = data_path / 'oauth_token.yaml'
    with file_path.open('r') as fin:
        rv = yaml.safe_load(fin)
    return rv
