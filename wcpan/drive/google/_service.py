from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from logging import getLogger
from pathlib import Path
from typing import override

from wcpan.drive.core.exceptions import NodeExistsError
from wcpan.drive.core.types import (
    ChangeAction,
    CreateHasher,
    FileService,
    MediaInfo,
    Node,
    PrivateDict,
    ReadableFile,
    WritableFile,
)

from ._lib import CHANGE_FIELDS, FILE_FIELDS, node_from_api, normalize_changes
from ._oauth import OAuth2Manager, OAuth2Storage
from ._readable import GoogleReadableFile
from ._writable import create_writable
from ._network import Network, create_network
from ._hasher import create_hasher
from .lib import fetch_child_by_name, fetch_node_by_id


@asynccontextmanager
async def create_service(*, client_secret: str, oauth_token: str):
    storage = OAuth2Storage(
        client_secret=Path(client_secret), oauth_token=Path(oauth_token)
    )
    oauth = OAuth2Manager(storage)
    async with create_network(oauth) as network:
        yield GoogleDriveFileService(network, oauth)


class GoogleDriveFileService(FileService):
    def __init__(self, network: Network, oauth: OAuth2Manager) -> None:
        self._network = network
        self._oauth = oauth

    @property
    @override
    def api_version(self) -> int:
        return 4

    @override
    async def get_initial_cursor(self) -> str:
        return "1"

    @override
    async def get_root(self) -> Node:
        from ._api.files import get

        rv = await get(self._network, "root", fields=FILE_FIELDS)
        rv["name"] = ""
        rv["parents"] = []
        node = node_from_api(rv)
        return node

    @override
    async def get_changes(
        self,
        cursor: str,
    ) -> AsyncIterator[tuple[list[ChangeAction], str]]:
        from ._api.changes import list_

        new_start_page_token = None
        page_token = cursor

        while new_start_page_token is None:
            rv = await list_(
                self._network,
                page_token=page_token,
                page_size=1000,
                restrict_to_my_drive=True,
                fields=CHANGE_FIELDS,
            )
            next_page_token = rv.get("nextPageToken", None)
            new_start_page_token = rv.get("newStartPageToken", None)
            changes = rv["changes"]

            cursor = (
                next_page_token if next_page_token is not None else new_start_page_token
            )
            changes = list(normalize_changes(changes))

            yield changes, cursor

            page_token = cursor

    @asynccontextmanager
    @override
    async def download_file(self, node: Node) -> AsyncIterator[ReadableFile]:
        async with GoogleReadableFile(self._network, node) as fin:
            yield fin

    @override
    async def create_directory(
        self,
        name: str,
        parent: Node,
        *,
        exist_ok: bool,
        private: PrivateDict | None,
    ) -> Node:
        # do not create again if there is a same file
        node = await fetch_child_by_name(
            self._network,
            name,
            parent.id,
        )
        if node:
            if exist_ok:
                getLogger(__name__).info(f"skipped (existing) {name}")
                return node
            else:
                raise NodeExistsError(node)

        from ._api.files import create_folder

        rv = await create_folder(
            self._network, folder_name=name, parent_id=parent.id, app_properties=private
        )
        node = await fetch_node_by_id(self._network, rv["id"])

        return node

    @asynccontextmanager
    @override
    async def upload_file(
        self,
        name: str,
        parent: Node,
        *,
        size: int | None,
        mime_type: str | None,
        media_info: MediaInfo | None,
        private: PrivateDict | None,
    ) -> AsyncIterator[WritableFile]:
        # do not upload if remote exists a same file
        node = await fetch_child_by_name(
            self._network,
            name,
            parent.id,
        )
        if node:
            raise NodeExistsError(node)

        async with create_writable(
            network=self._network,
            # get_node=_fetch_node_by_id,
            parent_id=parent.id,
            name=name,
            size=size,
            mime_type=mime_type,
            media_info=media_info,
            private=private,
        ) as fout:
            yield fout

    @override
    async def purge_trash(self) -> None:
        from ._api.files import empty_trash

        await empty_trash(self._network)

    @override
    async def delete(self, node: Node) -> None:
        from ._api.files import delete

        await delete(self._network, node.id)

    @override
    async def move(
        self,
        node: Node,
        *,
        new_parent: Node | None,
        new_name: str | None,
        trashed: bool | None,
    ) -> Node:
        if not node.parent_id:
            raise ValueError("cannot move root node")

        parent_id = node.parent_id if not new_parent else new_parent.id
        name = node.name if not new_name else new_name

        # no need to check conflict for trash mutation
        if trashed is None:
            # make sure it does not conflict to existing node
            new_node = await fetch_child_by_name(self._network, name, parent_id)
            if new_node:
                raise NodeExistsError(new_node)

        file_id = node.id
        name = new_name
        add_parents = None
        remove_parents = None
        if new_parent and new_parent.id != node.parent_id:
            add_parents = [new_parent.id]
            remove_parents = [node.parent_id]

        from ._api.files import update

        await update(
            self._network,
            file_id,
            name=name,
            add_parents=add_parents,
            remove_parents=remove_parents,
            trashed=trashed,
        )
        new_node = await fetch_node_by_id(self._network, node.id)
        return new_node

    @override
    async def get_hasher_factory(self) -> CreateHasher:
        return create_hasher

    @override
    async def is_authorized(self) -> bool:
        return bool(self._oauth.access_token)

    @override
    async def get_oauth_url(self) -> str:
        return self._oauth.build_authorization_url()

    @override
    async def set_oauth_token(self, token: str) -> None:
        await self._network.accept_oauth_code(token)

    @property
    def network(self) -> Network:
        return self._network
