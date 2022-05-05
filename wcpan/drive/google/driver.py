import asyncio
import contextlib
import hashlib
import re
from typing import (
    Any,
    AsyncGenerator,
    AsyncIterator,
    Generator,
    Optional,
)

from wcpan.logger import INFO, DEBUG, EXCEPTION, WARNING
from wcpan.drive.core.abc import (
    RemoteDriver,
    ReadableFile,
    WritableFile,
    Hasher,
)
from wcpan.drive.core.exceptions import (
    NodeConflictedError,
    InvalidNameError,
    NodeNotFoundError,
    ParentNotFoundError,
    UploadError,
)
from wcpan.drive.core.types import (
    ChangeDict,
    MediaInfo,
    Node,
    NodeDict,
    PrivateDict,
    ReadOnlyContext,
)

from .api import Client
from .exceptions import DownloadAbusiveFileError, ResponseError
from .util import FOLDER_MIME_TYPE, OAuth2Manager, OAuth2Storage


GoogleFileDict = dict[str, Any]


FILE_FIELDS = ','.join([
    'id',
    'name',
    'mimeType',
    'trashed',
    'parents',
    'createdTime',
    'modifiedTime',
    'md5Checksum',
    'size',
    'shared',
    'ownedByMe',
    'imageMediaMetadata/width',
    'imageMediaMetadata/height',
    'videoMediaMetadata/width',
    'videoMediaMetadata/height',
    'videoMediaMetadata/durationMillis',
    'appProperties',
])
CHANGE_FIELDS = ','.join([
    'nextPageToken',
    'newStartPageToken',
    f'changes(fileId,removed,file({FILE_FIELDS}))',
])
LIST_FIELDS = ','.join([
    'nextPageToken',
    f'files({FILE_FIELDS})',
])


class GoogleDriver(RemoteDriver):

    @classmethod
    def get_version_range(cls):
        return (3, 3)

    def __init__(self, context: ReadOnlyContext) -> None:
        storage = OAuth2Storage(context.config_path, context.data_path)
        self._oauth = OAuth2Manager(storage)
        self._timeout = 60
        self._client = None
        self._raii = None

    async def __aenter__(self) -> RemoteDriver:
        async with contextlib.AsyncExitStack() as stack:
            self._client = await stack.enter_async_context(
                Client(self._oauth, self._timeout))
            self._raii = stack.pop_all()
        return self

    async def __aexit__(self, exc_type, exc, tb) -> bool:
        await self._raii.aclose()
        self._client = None
        self._raii = None

    @property
    def remote(self):
        return None

    async def get_initial_check_point(self) -> str:
        return '1'

    async def fetch_root_node(self) -> Node:
        rv = await self._client.files.get('root', fields=FILE_FIELDS)
        rv = rv.json
        rv['name'] = None
        rv['parents'] = []
        node = node_from_api(rv)
        return node

    async def fetch_changes(self,
        check_point: str,
    ) -> AsyncGenerator[tuple[str, list[ChangeDict]], None]:
        new_start_page_token = None
        changes_list_args = {
            'page_token': check_point,
            'page_size': 1000,
            'restrict_to_my_drive': True,
            'fields': CHANGE_FIELDS,
        }

        while new_start_page_token is None:
            rv = await self._client.changes.list_(**changes_list_args)
            rv = rv.json
            next_page_token = rv.get('nextPageToken', None)
            new_start_page_token = rv.get('newStartPageToken', None)
            changes = rv['changes']

            check_point = next_page_token if next_page_token is not None else new_start_page_token
            changes = list(normalize_changes(changes))

            yield check_point, changes

            changes_list_args['page_token'] = check_point

    async def download(self, node: Node) -> ReadableFile:
        rf = GoogleReadableFile(self._client.files.download, node)
        return rf

    async def create_folder(self,
        parent_node: Node,
        folder_name: str,
        *,
        exist_ok: bool,
        private: Optional[PrivateDict],
    ) -> Node:
        # do not create again if there is a same file
        node = await self._fetch_node_by_name_from_parent_id(
            folder_name,
            parent_node.id_,
        )
        if node:
            if exist_ok:
                INFO('wcpan.drive.google') << 'skipped (existing)' << folder_name
                return node
            else:
                raise NodeConflictedError(node)

        api = self._client.files
        rv = await api.create_folder(folder_name=folder_name,
                                     parent_id=parent_node.id_,
                                     app_properties=private)
        rv = rv.json
        node = await self._fetch_node_by_id(rv['id'])

        return node

    async def upload(self,
        parent_node: Node,
        file_name: str,
        *,
        file_size: Optional[int],
        mime_type: Optional[str],
        media_info: Optional[MediaInfo],
        private: Optional[PrivateDict],
    ) -> WritableFile:
        # do not upload if remote exists a same file
        node = await self._fetch_node_by_name_from_parent_id(
            file_name,
            parent_node.id_,
        )
        if node:
            raise NodeConflictedError(node)

        api = self._client.files
        wf = GoogleWritableFile(
            initiate=api.initiate_uploading,
            upload=api.upload,
            get_status=api.get_upload_status,
            touch=api.create_empty_file,
            get_node=self._fetch_node_by_id,
            parent_id=parent_node.id_,
            name=file_name,
            timeout=self._timeout,
            size=file_size,
            mime_type=mime_type,
            media_info=media_info,
            private=private,
        )
        return wf

    async def trash_node(self, node: Node) -> None:
        await self._client.files.update(node.id_, trashed=True)

    async def rename_node(self,
        node: Node,
        *,
        new_parent: Optional[Node],
        new_name: Optional[str],
    ) -> Node:
        fnbnfpi = self._fetch_node_by_name_from_parent_id
        parent_id = node.parent_id if not new_parent else new_parent.id_
        name = node.name if not new_name else new_name
        # make sure it does not conflict to existing node
        new_node = await fnbnfpi(name, parent_id)
        if new_node:
            raise NodeConflictedError(new_node)

        kwargs = {
            'file_id': node.id_,
        }
        if new_name:
            kwargs['name'] = new_name
        if new_parent and new_parent.id_ != node.parent_id:
            kwargs['add_parents'] = [new_parent.id_]
            kwargs['remove_parents'] = [node.parent_id]

        dummy_rv = await self._client.files.update(**kwargs)
        node = await self._fetch_node_by_id(node.id_)
        return node

    async def get_hasher(self) -> Hasher:
        return PicklableHasher()

    async def is_authorized(self) -> bool:
        return self._oauth.access_token is not None

    async def get_oauth_url(self) -> str:
        return self._oauth.build_authorization_url()

    async def set_oauth_token(self, token: str) -> None:
        await self._client.accept_oauth_code(token)

    async def _fetch_node_by_name_from_parent_id(self,
        name: str,
        parent_id: str,
    ) -> Node:
        safe_name = re.sub(r"[\\']", r"\\\g<0>", name)
        query = ' and '.join([
            f"'{parent_id}' in parents",
            f"name = '{safe_name}'",
            f'trashed = false',
        ])
        fields = f'files({FILE_FIELDS})'
        try:
            rv = await self._client.files.list_(q=query, fields=fields)
        except ResponseError as e:
            if e.status == '400':
                DEBUG('wcpan.drive.google') << 'invalid query string:' << query
                raise InvalidNameError(name) from e
            if e.status == '404':
                raise ParentNotFoundError(parent_id) from e
            raise

        rv = rv.json
        files = rv['files']
        if not files:
            return None

        node = node_from_api(files[0])
        return node

    async def _fetch_node_by_id(self, node_id: str) -> Node:
        try:
            rv = await self._client.files.get(node_id, fields=FILE_FIELDS)
        except ResponseError as e:
            if e.status == '404':
                raise NodeNotFoundError(node_id) from e
            raise
        rv = rv.json
        node = node_from_api(rv)
        return node

    async def _set_node_parent_by_id(self, node: Node, parent_id: str) -> None:
        remove_parents = [_ for _ in node.parent_list if _ != parent_id]
        api = self._client.files
        await api.update(node.id_, remove_parents=remove_parents)

    async def _force_update_by_id(self, node_id: str) -> None:
        await self._client.files.update(node_id, trashed=False)

    async def _fetch_children(self, parent_id: str) -> list[Node]:
        query = ' and '.join([
            f"'{parent_id}' in parents",
        ])
        page_token = None
        files = []
        try:
            while True:
                rv = await self._client.files.list_(
                    q=query,
                    fields=LIST_FIELDS,
                    page_size=1000,
                    page_token=page_token,
                )
                rv = rv.json
                files.extend(rv['files'])
                page_token = rv.get('nextPageToken', None)
                if not page_token:
                    break
        except ResponseError as e:
            if e.status == '404':
                raise ParentNotFoundError(parent_id) from e
            raise

        node_list = [node_from_api(_) for _ in files]
        return node_list


class GoogleReadableFile(ReadableFile):

    def __init__(self, download: Any, node: Node) -> None:
        self._download = download
        self._node = node
        self._offset = 0
        self._response = None
        self._rsps = None

    async def __aenter__(self) -> ReadableFile:
        return self

    async def __aexit__(self, type_, exc, tb) -> bool:
        await self._close_response()

    async def __aiter__(self) -> AsyncIterator[bytes]:
        # no need to make request for an empty file
        if self._node.size <= 0:
            return

        await self._open_response()
        async for chunk in self._response.chunks():
            yield chunk

    async def read(self, length: int) -> bytes:
        # nothing to read from an empty file
        if self._node.size <= 0:
            return b''

        await self._open_response()
        return await self._response.read(length)

    async def seek(self, offset: int) -> None:
        # nop for seeking an empty file
        if self._node.size <= 0:
            return

        self._offset = offset
        await self._close_response()
        await self._open_response()

    async def node(self) -> Node:
        return self._node

    async def _download_from_offset(self) -> 'aiohttp.StreamResponse':
        try:
            return await self._download(
                file_id=self._node.id_,
                range_=(self._offset, self._node.size),
                acknowledge_abuse=False,
            )
        except DownloadAbusiveFileError:
            # FIXME automatically accept abuse files for now
            WARNING('wcpan.drive.google') << f'{self._node.id_} is an abusive file'
            return await self._download(
                file_id=self._node.id_,
                range_=(self._offset, self._node.size),
                acknowledge_abuse=True,
            )

    async def _open_response(self) -> None:
        if not self._response:
            async with contextlib.AsyncExitStack() as stack:
                self._response = await stack.enter_async_context(
                    await self._download_from_offset(),
                )
                self._rsps = stack.pop_all()

    async def _close_response(self) -> None:
        if self._response:
            await self._rsps.aclose()
            self._response = None
            self._rsps = None


class GoogleWritableFile(WritableFile):

    def __init__(self,
        initiate: Any,
        upload: Any,
        get_status: Any,
        get_node: Any,
        touch: Any,
        parent_id: str,
        name: str,
        timeout: float,
        size: int = None,
        mime_type: str = None,
        media_info: MediaInfo = None,
        private: PrivateDict = None,
    ) -> None:
        self._initiate = initiate
        self._upload = upload
        self._get_status = get_status
        self._get_node = get_node
        self._touch = touch
        self._parent_id = parent_id
        self._name = name
        self._private = private
        self._timeout = timeout
        self._size = size
        self._mime_type = mime_type
        self._media_info = media_info
        self._url = None
        self._offset = None
        self._queue = asyncio.Queue(maxsize=1)
        self._bg = None
        self._rv = None

    async def __aenter__(self) -> WritableFile:
        if self._size > 0:
            self._url = await self._get_session_url()
            self._bg = None
        else:
            self._url = None
            self._bg = self._touch_empty()
        self._offset = 0
        self._queue = asyncio.Queue(maxsize=1)
        self._rv = None

        return self

    async def __aexit__(self, type_, exc, tb) -> bool:
        if self._bg:
            rv = await self._bg
            self._rv = rv.json
        else:
            # error happened before the first write
            self._rv = None
        self._bg = None
        self._queue = None
        self._offset = None
        self._url = None

    async def tell(self) -> int:
        return await self._get_offset()

    async def seek(self, offset: int) -> None:
        self._offset = offset
        await self._close_request()
        await self._open_request()

    async def write(self, chunk: bytes) -> int:
        await self._open_request()
        feed = self._queue.put(chunk)
        feed = asyncio.wait_for(feed, timeout=self._timeout)
        feed = asyncio.create_task(feed)
        await asyncio.wait([feed, self._bg],
                           return_when=asyncio.FIRST_COMPLETED)
        if not feed.done():
            # background connection ended
            feed.cancel()
        # consume exception
        feed.result()
        return len(chunk)

    async def node(self) -> Optional[Node]:
        if not self._rv:
            return None
        node = await self._get_node(self._rv['id'])
        return node

    async def _close_request(self) -> None:
        if not self._bg:
            return None
        if not self._bg.done():
            self._bg.cancel()
        try:
            rv = await self._bg
            return rv
        except (Exception, asyncio.CancelledError) as e:
            EXCEPTION('wcpan.drive.google', e) << 'close'
        finally:
            self._queue = asyncio.Queue(maxsize=1)
            self._bg = None

    async def _open_request(self) -> None:
        if self._bg:
            return
        f = self._upload_to()
        self._bg = asyncio.create_task(f)

    async def _produce(self) -> AsyncGenerator[bytes, None]:
        while True:
            async with self._get_one_chunk() as chunk:
                if not chunk:
                    break
                yield chunk

    @contextlib.asynccontextmanager
    async def _get_one_chunk(self) -> AsyncGenerator[bytes, None]:
        chunk = await self._queue.get()
        try:
            yield chunk
        finally:
            self._queue.task_done()

    async def _get_session_url(self) -> str:
        rv = await self._initiate(
            file_name=self._name,
            total_file_size=self._size,
            parent_id=self._parent_id,
            mime_type=self._mime_type,
            media_info=self._media_info,
            app_properties=self._private,
        )
        url = rv.get_header('Location')
        return url

    async def _upload_to(self) -> 'aiohttp.ClientResponse':
        try:
            rv = await self._upload(
                self._url,
                producer=self._produce,
                offset=self._offset,
                total_file_size=self._size,
                mime_type=self._mime_type,
            )
        except ResponseError as e:
            if e.status == '404':
                raise UploadError('the upload session has been expired') from e
            raise
        return rv

    async def _get_offset(self) -> int:
        try:
            rv = await self._get_status(self._url, self._size)
        except ResponseError as e:
            if e.status == '410':
                # This means the temporary URL has been cleaned up by Google
                # Drive, so the client has to start over again.
                raise UploadError('the upload session has been expired') from e
            raise

        if rv.status != '308':
            raise UploadError(f'invalid upload status: {rv.status}')
        
        try:
            rv = rv.get_header('Range')
        except KeyError:
            # no data yet
            return 0

        rv = re.match(r'bytes=0-(\d+)', rv)
        if not rv:
            raise UploadError('invalid upload range')
        rv = int(rv.group(1))
        return rv

    async def _touch_empty(self) -> 'aiohttp.ClientResponse':
        rv = await self._touch(
            self._name,
            parent_id=self._parent_id,
            mime_type=self._mime_type,
            app_properties=self._private,
        )
        return rv


class PicklableHasher(Hasher):

    def __init__(self):
        self._hasher = hashlib.md5()

    def __getstate__(self):
        return hashlib.md5

    def __setstate__(self, hasher_class):
        self._hasher = hasher_class()

    def update(self, data):
        self._hasher.update(data)

    def hexdigest(self):
        return self._hasher.hexdigest()

    def digest(self):
        return self._hasher.digest()

    def copy(self):
        return self._hasher.copy()


def normalize_changes(
    change_list: list[GoogleFileDict],
) -> Generator[ChangeDict, None, None]:
    for change in change_list:
        is_removed = change['removed']
        if is_removed:
            yield {
                'removed': True,
                'id': change['fileId'],
            }
            continue

        file_ = change['file']
        is_shared = file_['shared']
        is_owned_by_me = file_['ownedByMe']
        if is_shared or not is_owned_by_me:
            continue

        file_ = dict_from_api(file_)
        yield {
            'removed': False,
            'node': file_,
        }


def dict_from_api(data: GoogleFileDict) -> NodeDict:
    id_ = data['id']

    is_folder = data['mimeType'] == FOLDER_MIME_TYPE

    size = data.get('size', None)
    if size is not None:
        size = int(size)

    private = data.get('appProperties', None)

    image = None
    if private and 'image' in private:
        width, height = private['image'].split(' ')
        image = {
            'width': int(width),
            'height': int(height),
        }
        del private['image']
    if not image and 'imageMediaMetadata' in data:
        image = {
            'width': data['imageMediaMetadata']['width'],
            'height': data['imageMediaMetadata']['height'],
        }

    video = None
    if private and 'video' in private:
        width, height, ms_duration = private['video'].split(' ')
        video = {
            'width': int(width),
            'height': int(height),
            'ms_duration': int(ms_duration),
        }
        del private['video']
    if not video and 'videoMediaMetadata' in data:
        video = {
            'width': data['videoMediaMetadata']['width'],
            'height': data['videoMediaMetadata']['height'],
            'ms_duration': data['videoMediaMetadata']['durationMillis'],
        }

    return {
        'id': id_,
        'name': data['name'],
        'trashed': data['trashed'],
        'created': data['createdTime'],
        'modified': data['modifiedTime'],
        'parent_list': data.get('parents', None),
        'is_folder': is_folder,
        'mime_type': None if is_folder else data['mimeType'],
        'hash': data.get('md5Checksum', None),
        'size': size,
        'image': image,
        'video': video,
        'private': private,
    }


def node_from_api(data: GoogleFileDict) -> Node:
    node_data = dict_from_api(data)
    return Node.from_dict(node_data)
