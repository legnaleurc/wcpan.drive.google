import asyncio
from asyncio import Queue, TaskGroup, CancelledError
from collections.abc import AsyncIterator, Callable, Awaitable, Coroutine
from contextlib import asynccontextmanager
from functools import partial
from typing import override

from aiohttp import ClientResponseError
from wcpan.drive.core.exceptions import NodeNotFoundError
from wcpan.drive.core.types import MediaInfo, Node, PrivateDict, WritableFile

from ._network import Network
from ._lib import node_from_api
from .exceptions import UploadError
from .lib import fetch_node_by_id


type GetNode = Callable[[str], Awaitable[Node]]
type GetOffset = Callable[[], Awaitable[int]]
type Consume = Callable[[int], Coroutine[None, None, str]]


@asynccontextmanager
async def create_writable(
    *,
    network: Network,
    parent_id: str,
    name: str,
    size: int | None = None,
    mime_type: str | None = None,
    media_info: MediaInfo | None = None,
    private: PrivateDict | None = None,
) -> AsyncIterator[WritableFile]:
    if size is None:
        raise Exception("unknown size not supported")
    if size <= 0:
        yield await create_empty(
            network=network,
            name=name,
            parent_id=parent_id,
            mime_type=mime_type,
            private=private,
        )
        return
    async with create_pipe(
        network=network,
        parent_id=parent_id,
        name=name,
        size=size,
        mime_type=mime_type,
        media_info=media_info,
        private=private,
    ) as fout:
        yield fout


async def create_empty(
    *,
    network: Network,
    name: str,
    parent_id: str,
    mime_type: str | None = None,
    private: PrivateDict | None = None,
) -> WritableFile:
    from ._api.files import create_empty_file

    rv = await create_empty_file(
        network,
        name,
        parent_id=parent_id,
        mime_type=mime_type,
        app_properties=private,
    )
    node = node_from_api(rv)
    return GoogleEmptyWritableFile(node)


class GoogleEmptyWritableFile(WritableFile):
    """
    Writing an empty file does not need any operation.
    """

    def __init__(self, node: Node):
        self._node = node

    @override
    async def write(self, chunk: bytes) -> int:
        return 0

    @override
    async def seek(self, offset: int) -> int:
        return 0

    @override
    async def tell(self) -> int:
        return 0

    @override
    async def flush(self) -> None:
        return

    @override
    async def node(self) -> Node:
        return self._node


@asynccontextmanager
async def create_pipe(
    *,
    network: Network,
    parent_id: str,
    name: str,
    size: int,
    timeout: float | None = None,
    mime_type: str | None = None,
    media_info: MediaInfo | None = None,
    private: PrivateDict | None = None,
) -> AsyncIterator[WritableFile]:
    from ._api.files import initiate_uploading

    url = await initiate_uploading(
        network,
        file_name=name,
        total_file_size=size,
        parent_id=parent_id,
        mime_type=mime_type,
        media_info=media_info,
        app_properties=private,
    )
    async with TaskGroup() as task_group:
        queue = Queue[bytes](1)

        get_offset = partial(_get_offset, network=network, url=url, size=size)
        consume = partial(
            _consume,
            queue,
            network=network,
            url=url,
            size=size,
            mime_type=mime_type,
        )
        get_node = partial(fetch_node_by_id, network)

        yield GooglePipeWritableFile(
            queue=queue,
            task_group=task_group,
            consume=consume,
            get_offset=get_offset,
            get_node=get_node,
            timeout=timeout,
        )


class GooglePipeWritableFile(WritableFile):
    def __init__(
        self,
        *,
        queue: Queue[bytes],
        task_group: TaskGroup,
        consume: Consume,
        get_offset: GetOffset,
        get_node: GetNode,
        timeout: float | None,
    ) -> None:
        self._task_group = task_group
        self._queue = queue
        self._consume = consume
        self._get_offset = get_offset
        self._get_node = get_node
        self._consumer = task_group.create_task(consume(0))
        self._timeout = timeout

    @override
    async def write(self, chunk: bytes) -> int:
        async with asyncio.timeout(self._timeout):
            await self._queue.put(chunk)
        return len(chunk)

    @override
    async def flush(self) -> None:
        await self._queue.join()

    @override
    async def tell(self) -> int:
        await self.flush()
        return await self._get_offset()

    @override
    async def seek(self, offset: int) -> int:
        await self.flush()
        self._consumer.cancel()
        try:
            await self._consumer
        except CancelledError:
            pass
        self._consumer = self._task_group.create_task(self._consume(offset))
        return offset

    @override
    async def node(self) -> Node:
        try:
            id_ = await self._consumer
        except Exception as e:
            raise UploadError("upload failed") from e
        try:
            node = await self._get_node(id_)
        except Exception as e:
            raise NodeNotFoundError(id_) from e
        return node


async def _get_offset(*, network: Network, url: str, size: int) -> int:
    from ._api.files import get_upload_status

    try:
        status, offset = await get_upload_status(network, url, size)
    except ClientResponseError as e:
        if e.status == 404:
            raise UploadError("the upload session does not exist") from e
        if e.status == 410:
            # This means the temporary URL has been cleaned up by Google
            # Drive, so the client has to start over again.
            raise UploadError("the upload session has been expired") from e
        raise

    if status != 308:
        raise UploadError(f"invalid upload status: {status}")

    return offset


async def _consume(
    queue: Queue[bytes],
    offset: int,
    *,
    network: Network,
    url: str,
    size: int,
    mime_type: str | None,
):
    from ._api.files import upload

    id_ = await upload(
        network,
        url,
        producer=_produce(queue),
        offset=offset,
        total_file_size=size,
        mime_type=mime_type,
    )
    return id_


async def _produce(queue: Queue[bytes]) -> AsyncIterator[bytes]:
    while True:
        async with _dequeue(queue) as chunk:
            if not chunk:
                break
            yield chunk


@asynccontextmanager
async def _dequeue(queue: Queue[bytes]) -> AsyncIterator[bytes]:
    chunk = await queue.get()
    try:
        yield chunk
    finally:
        queue.task_done()
