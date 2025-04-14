import json
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any, cast
from unittest import IsolatedAsyncioTestCase, TestCase
from unittest.mock import MagicMock

from aiohttp.test_utils import AioHTTPTestCase
from aiohttp.web import Application, HTTPBadRequest, Request, Response, json_response

from wcpan.drive.google._oauth import OAuth2Manager, OAuth2Storage
from wcpan.drive.google.exceptions import CredentialFileError, TokenFileError


class OAuth2StorageTestCase(TestCase):
    def setUp(self):
        tmp = self.enterContext(TemporaryDirectory())
        self._work_path = Path(tmp)
        self._client_secret = self._work_path / "client_secret.json"
        self._oauth_token = self._work_path / "oauth_token.json"
        self._storage = OAuth2Storage(
            client_secret=self._client_secret,
            oauth_token=self._oauth_token,
        )

    def testLoadError1(self):
        with self.assertRaises(FileNotFoundError):
            self._storage.load_oauth2_config()

    def testLoadError2(self):
        write_config(self._client_secret, {})
        with self.assertRaises(CredentialFileError):
            self._storage.load_oauth2_config()

    def testLoadError3(self):
        write_token(
            self._oauth_token,
            {
                "version": -1,
            },
        )
        with self.assertRaises(TokenFileError):
            self._storage.load_oauth2_token()

    def testLoadConfig(self):
        write_default_config(self._client_secret)
        rv = self._storage.load_oauth2_config()
        self.assertEqual(rv["client_id"], "__ID__")
        self.assertEqual(rv["client_secret"], "__SECRET__")
        self.assertEqual(rv["redirect_uri"], "__REDIRECT_URI__")
        self.assertEqual(rv["token_uri"], "__TOKEN_URI__")
        self.assertEqual(rv["auth_uri"], "__AUTH_URI__")

    def testLoadToken(self):
        write_token(
            self._oauth_token,
            {
                "version": 1,
                "access_token": "__ACCESS__",
                "refresh_token": "__REFRESH__",
            },
        )
        rv = self._storage.load_oauth2_token()
        self.assertEqual(rv["access_token"], "__ACCESS__")
        self.assertEqual(rv["refresh_token"], "__REFRESH__")

    def testLoadEmptyToken(self):
        rv = self._storage.load_oauth2_token()
        self.assertEqual(rv["access_token"], "")
        self.assertEqual(rv["refresh_token"], "")

    def testSaveToken(self):
        self._storage.save_oauth2_token(
            access_token="__ACCESS__",
            refresh_token="__REFRESH__",
        )
        rv = read_token(self._oauth_token)
        self.assertEqual(rv["version"], 1)
        self.assertEqual(rv["access_token"], "__ACCESS__")
        self.assertEqual(rv["refresh_token"], "__REFRESH__")


class OAuth2ManagerTestCase(IsolatedAsyncioTestCase):
    async def testAuthorizeStatus(self):
        storage = create_mocked_storage(config={}, token={})
        oauth = OAuth2Manager(storage)
        self.assertIsNone(oauth.access_token)
        self.assertIsNone(oauth.refresh_token)

        storage = create_mocked_storage(config={})
        oauth = OAuth2Manager(storage)
        self.assertEqual(oauth.access_token, "__ACCESS__")
        self.assertEqual(oauth.refresh_token, "__REFRESH__")

    async def testGetOAuthUrl(self):
        storage = create_mocked_storage()
        oauth = OAuth2Manager(storage)
        url = oauth.build_authorization_url()
        self.assertEqual(
            url,
            "__AUTH_URI__?redirect_uri=__REDIRECT_URI__&client_id=__ID__&response_type=code&access_type=offline&prompt=consent&scope=https%3A%2F%2Fwww.googleapis.com%2Fauth%2Fdrive",
        )


class AcceptAnswerTestCase(AioHTTPTestCase):
    async def get_application(self) -> Application:
        async def on_token(request: Request) -> Response:
            if request.content_type != "application/x-www-form-urlencoded":
                return HTTPBadRequest()

            data = await request.post()
            expected = [
                ("redirect_uri", "__REDIRECT_URI__"),
                ("code", "__CODE__"),
                ("client_id", "__ID__"),
                ("client_secret", "__SECRET__"),
                ("grant_type", "authorization_code"),
            ]
            for key, value in expected:
                rv = data.getone(key)
                if rv != value:
                    raise HTTPBadRequest()

            return json_response(
                {
                    "access_token": "__NEW_ACCESS__",
                    "refresh_token": "__NEW_REFRESH__",
                }
            )

        app = Application()
        app.router.add_post("/api/v1/token", on_token)
        return app

    async def testCall(self):
        storage = create_mocked_storage(
            config={
                "token_uri": str(self.server.make_url("/api/v1/token")),
                "redirect_uri": "__REDIRECT_URI__",
                "client_id": "__ID__",
                "client_secret": "__SECRET__",
            }
        )
        oauth = OAuth2Manager(storage)

        answer = "http://localhost/?code=__CODE__&scope=__SCOPE__"

        await oauth.set_authenticated_token(self.client.session, answer)
        self.assertEqual(oauth.access_token, "__NEW_ACCESS__")
        self.assertEqual(oauth.refresh_token, "__NEW_REFRESH__")
        expect(storage.save_oauth2_token).assert_called()


class RefreshTestCase(AioHTTPTestCase):
    async def get_application(self) -> Application:
        async def on_token(request: Request) -> Response:
            if request.content_type != "application/x-www-form-urlencoded":
                return HTTPBadRequest()

            data = await request.post()
            expected = [
                ("client_id", "__ID__"),
                ("client_secret", "__SECRET__"),
                ("refresh_token", "__REFRESH__"),
                ("grant_type", "refresh_token"),
            ]
            for key, value in expected:
                rv = data.getone(key)
                if rv != value:
                    raise HTTPBadRequest()

            return json_response(
                {
                    "access_token": "__NEW_ACCESS__",
                }
            )

        app = Application()
        app.router.add_post("/api/v1/token", on_token)
        return app

    async def testCall(self):
        storage = create_mocked_storage(
            config={
                "token_uri": str(self.server.make_url("/api/v1/token")),
                "redirect_uri": "__REDIRECT_URI__",
                "client_id": "__ID__",
                "client_secret": "__SECRET__",
            }
        )
        oauth = OAuth2Manager(storage)

        await oauth.renew_token(self.client.session)
        self.assertEqual(oauth.access_token, "__NEW_ACCESS__")
        self.assertEqual(oauth.refresh_token, "__REFRESH__")
        expect(storage.save_oauth2_token).assert_called()


def expect(o: object) -> MagicMock:
    return cast(MagicMock, o)


def create_mocked_storage(
    *, config: dict[str, str] | None = None, token: dict[str, str] | None = None
) -> OAuth2Storage:
    storage = cast(OAuth2Storage, MagicMock(spec=OAuth2Storage))
    if config is None:
        config = {
            "redirect_uri": "__REDIRECT_URI__",
            "auth_uri": "__AUTH_URI__",
            "token_uri": "__TOKEN_URI__",
            "client_id": "__ID__",
            "client_secret": "__SECRET__",
        }
    if token is None:
        token = {
            "access_token": "__ACCESS__",
            "refresh_token": "__REFRESH__",
        }
    expect(storage.load_oauth2_config).return_value = config
    expect(storage.load_oauth2_token).return_value = token
    return storage


def write_config(client_secret: Path, dict_: dict[str, Any]) -> None:
    with client_secret.open("w") as fout:
        json.dump(dict_, fout)


def write_default_config(client_secret: Path) -> None:
    write_config(
        client_secret,
        {
            "web": {
                "redirect_uris": [
                    "__REDIRECT_URI__",
                ],
                "auth_uri": "__AUTH_URI__",
                "token_uri": "__TOKEN_URI__",
                "client_id": "__ID__",
                "client_secret": "__SECRET__",
            },
        },
    )


def write_token(oauth_token: Path, dict_: dict[str, Any]) -> None:
    with oauth_token.open("w") as fout:
        json.dump(dict_, fout)


def read_token(oauth_token: Path) -> dict[str, Any]:
    with oauth_token.open("r") as fin:
        rv = json.load(fin)
    return rv
