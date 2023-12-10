from collections.abc import AsyncIterator
from contextlib import AsyncExitStack, asynccontextmanager
from functools import partial
from logging import getLogger
from typing import Any, override

from aiohttp import ClientResponse
from wcpan.drive.core.types import ReadableFile, Node

from .exceptions import DownloadAbusiveFileError
from ._network import Network


class GoogleReadableFile(ReadableFile):
    def __init__(self, network: Network, node: Node) -> None:
        self._network = network
        self._node = node
        self._offset = 0
        self._response = None
        self._rsps = None

    async def __aenter__(self) -> ReadableFile:
        return self

    async def __aexit__(self, type: Any, exc: Any, tb: Any) -> None:
        await self._close_response()

    @override
    async def __aiter__(self) -> AsyncIterator[bytes]:
        # no need to make request for an empty file
        if self._node.size <= 0:
            return

        await self._open_response()
        assert self._response
        async for chunk in self._response.content.iter_any():
            yield chunk

    @override
    async def read(self, length: int) -> bytes:
        # nothing to read from an empty file
        if self._node.size <= 0:
            return b""

        await self._open_response()
        assert self._response
        return await self._response.content.read(length)

    @override
    async def seek(self, offset: int) -> int:
        # nop for seeking an empty file
        if self._node.size <= 0:
            return 0

        self._offset = offset
        await self._close_response()
        await self._open_response()
        return self._offset

    @override
    async def node(self) -> Node:
        return self._node

    @asynccontextmanager
    async def _download_from_offset(self) -> AsyncIterator[ClientResponse]:
        from ._api.files import download

        dl = partial(
            download, self._network, self._node.id, (self._offset, self._node.size)
        )

        try:
            async with dl(acknowledge_abuse=False) as response:
                yield response
        except DownloadAbusiveFileError:
            # FIXME automatically accept abuse files for now
            getLogger(__name__).warning(f"{self._node.id} is an abusive file")
            async with dl(acknowledge_abuse=True) as response:
                yield response

    async def _open_response(self) -> None:
        if not self._response:
            async with AsyncExitStack() as stack:
                self._response = await stack.enter_async_context(
                    self._download_from_offset()
                )
                self._rsps = stack.pop_all()

    async def _close_response(self) -> None:
        if self._response:
            assert self._rsps
            await self._rsps.aclose()
            self._response = None
            self._rsps = None
