import asyncio
import contextlib as cl
import functools as ft
import mimetypes
import os
import os.path as op
import re
from typing import (Any, AsyncGenerator, Awaitable, Dict, Generator, List,
                    Optional, Text, Tuple, Union)

from wcpan.logger import INFO, WARNING, DEBUG, EXCEPTION

from .api import Client
from .cache import Cache, Node, node_from_api, dict_from_node
from .network import ContentProducer, ResponseError, Response, NetworkError
from .util import Settings, GoogleDriveError, CHUNK_SIZE


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
    'imageMediaMetadata',
    'videoMediaMetadata',
])
CHANGE_FIELDS = ','.join([
    'nextPageToken',
    'newStartPageToken',
    f'changes(fileId,removed,file({FILE_FIELDS}))',
])


class Drive(object):

    def __init__(self, conf_path: Text = None, timeout: int = 60) -> None:
        self._settings = Settings(conf_path)
        self._timeout = timeout
        self._client = None
        self._db = None
        self._sync_lock = asyncio.Lock()
        self._raii = None

    async def __aenter__(self) -> 'Drive':
        async with cl.AsyncExitStack() as stack:
            self._client = await stack.enter_async_context(
                Client(self._settings, self._timeout))
            dsn = self._settings['nodes_database_file']
            self._db = await stack.enter_async_context(Cache(dsn))
            self._raii = stack.pop_all()
        return self

    async def __aexit__(self, exc_type, exc, tb) -> bool:
        await self._raii.aclose()
        self._client = None
        self._db = None
        self._raii = None

    async def sync(self,
        check_point: int = None,
    ) -> AsyncGenerator[Dict[Text, Any], None]:
        async with self._sync_lock:
            async for change in self._real_sync(check_point):
                yield change

    async def get_root_node(self) -> Node:
        return await self._db.get_root_node()

    async def get_node_by_id(self, node_id: Text) -> Node:
        return await self._db.get_node_by_id(node_id)

    async def get_node_by_path(self, path: Text) -> Node:
        return await self._db.get_node_by_path(path)

    async def get_path(self, node: Node) -> Text:
        return await self._db.get_path_by_id(node.id_)

    async def get_path_by_id(self, node_id: Text) -> Text:
        return await self._db.get_path_by_id(node_id)

    async def get_node_by_name_from_parent_id(self,
        name: Text,
        parent_id: Text,
    ) -> Node:
        return await self._db.get_node_by_name_from_parent_id(name, parent_id)

    async def get_node_by_name_from_parent(self,
        name: Text,
        parent: Node,
    ) -> Node:
        return await self._db.get_node_by_name_from_parent_id(name, parent.id_)

    async def get_children(self, node: Node) -> List[Node]:
        return await self._db.get_children_by_id(node.id_)

    async def get_children_by_id(self, node_id: Text) -> List[Node]:
        return await self._db.get_children_by_id(node_id)

    async def find_nodes_by_regex(self, pattern: Text) -> List[Node]:
        return await self._db.find_nodes_by_regex(pattern)

    async def find_duplicate_nodes(self) -> List[Node]:
        return await self._db.find_duplicate_nodes()

    async def find_orphan_nodes(self) -> List[Node]:
        return await self._db.find_orphan_nodes()

    async def find_multiple_parents_nodes(self) -> List[Node]:
        return await self._db.find_multiple_parents_nodes()

    async def download_by_id(self, node_id: Text) -> 'ReadableFile':
        node = await self.get_node_by_id(node_id)
        return await self.download(node)

    async def download(self, node: Node) -> 'ReadableFile':
        # sanity check
        if not node:
            raise ValueError('node is none')
        if node.is_folder:
            raise ValueError('node should be a file')

        rf = ReadableFile(self._client.files.download, node)
        return rf

    async def create_folder(self,
        parent_node: Node,
        folder_name: Text,
        exist_ok: bool = False,
    ) -> Node:
        # sanity check
        if not parent_node:
            raise UploadError('invalid parent node')
        if not parent_node.is_folder:
            raise UploadError('invalid parent node')
        if not folder_name:
            raise UploadError('invalid folder name')

        # do not create again if there is a same file
        node = await self.fetch_node_by_name_from_parent_id(folder_name,
                                                            parent_node.id_)
        if node:
            if exist_ok:
                INFO('wcpan.drive.google') << 'skipped (existing)' << folder_name
                return node
            else:
                raise FileConflictedError(node)

        api = self._client.files
        rv = await api.create_folder(folder_name=folder_name,
                                     parent_id=parent_node.id_)
        rv = rv.json
        node = await self.fetch_node_by_id(rv['id'])

        return node

    async def upload_by_id(self,
        parent_id: Text,
        file_name: Text,
        file_size: int = None,
        mime_type: Text = None,
    ) -> 'WritableFile':
        node = await self.get_node_by_id(parent_id)
        return await self.upload(node, file_name, file_size, mime_type)

    async def upload(self,
        parent_node: Node,
        file_name: Text,
        file_size: int = None,
        mime_type: Text = None,
    ) -> 'WritableFile':
        # sanity check
        if not parent_node:
            raise UploadError('invalid parent node')
        if not parent_node.is_folder:
            raise UploadError('invalid parent node')
        if not file_name:
            raise UploadError('invalid file name')

        # do not upload if remote exists a same file
        node = await self.fetch_node_by_name_from_parent_id(file_name,
                                                            parent_node.id_)
        if node:
            raise FileConflictedError(node)

        api = self._client.files
        wf = WritableFile(initiate=api.initiate_uploading,
                          upload=api.upload,
                          get_status=api.get_upload_status,
                          touch=api.create_empty_file,
                          parent_id=parent_node.id_,
                          name=file_name,
                          timeout=self._timeout,
                          size=file_size,
                          mime_type=mime_type)
        return wf

    async def fetch_node_by_name_from_parent_id(self,
        name: Text,
        parent_id: Text,
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

    async def fetch_node_by_id(self, node_id: Text) -> Node:
        try:
            rv = await self._client.files.get(node_id, fields=FILE_FIELDS)
        except ResponseError as e:
            if e.status == '404':
                raise NodeNotFoundError(node_id) from e
            raise
        rv = rv.json
        node = node_from_api(rv)
        return node

    async def trash_node_by_id(self, node_id: Text) -> Node:
        node = await self.get_root_node()
        if node_id == node.id_:
            return
        await self._client.files.update(node_id, trashed=True)

        node = await self.get_node_by_id(node_id)
        node.trashed = True
        return node

    async def trash_node(self, node: Node) -> Node:
        return await self.trash_node_by_id(node.id_)

    async def rename_node_by_path(self, src_path: Text, dst_path: Text) -> Node:
        node = await self.get_node_by_path(src_path)
        if not node:
            raise NodeNotFoundError(src_path)
        return await self.rename_node(node, dst_path)

    async def rename_node_by_id(self, node_id: Text, dst_path: Text) -> Node:
        node = await self.get_node_by_id(node_id)
        return await self.rename_node(node, dst_path)

    async def rename_node(self, src_node: Node, dst_path: Text) -> Node:
        '''
        Rename or move `src_node` to `dst_path`. `dst_path` can be a file name
        or an absolute path.
        If `dst_path` is a file and already exists, `FileConflictedError` will
        be raised.
        If `dst_path` is a folder, `src_node` will be moved to there without
        renaming.
        If `dst_path` does not exist yet, `src_node` will be moved and rename to
        `dst_path`.
        '''
        # sanity check
        if not src_node:
            raise ValueError('source node is none')

        parent, dst_name = await self._get_dst_info(dst_path)
        await self._inner_rename_node(src_node, parent, dst_name)

        node = await self.fetch_node_by_id(src_node.id_)
        return node

    async def set_node_parent_by_id(self, node: Node, parent_id: Text) -> None:
        remove_parents = [_ for _ in node.parent_list if _ != parent_id]
        api = self._client.files
        await api.update(node.id_, remove_parents=remove_parents)


    async def walk(self,
        node: Node,
    ) -> AsyncGenerator[Tuple[Node, List[Node], List[Node]], None]:
        if not node.is_folder:
            return
        q = [node]
        while q:
            node = q[0]
            del q[0]
            children = await self.get_children(node)
            folders = list(filter(lambda _: _.is_folder, children))
            files = list(filter(lambda _: _.is_file, children))
            yield node, folders, files
            q.extend(folders)

    async def _real_sync(self,
        check_point: int,
    ) -> AsyncGenerator[Dict[Text, Any], None]:
        dry_run = check_point is not None
        if dry_run and check_point > 0:
            check_point = str(check_point)
        else:
            try:
                check_point = await self._db.get_metadata('check_point')
            except KeyError:
                check_point = '1'

        # first time, get root node
        if not dry_run and check_point == '1':
            rv = await self._client.files.get('root', fields=FILE_FIELDS)
            rv = rv.json
            rv['name'] = None
            rv['parents'] = []
            node = node_from_api(rv)
            await self._db.insert_node(node)

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

            if not dry_run:
                await self._db.apply_changes(changes, check_point)

            changes_list_args['page_token'] = check_point

            for change in transform_changes(changes):
                yield change

    async def _get_dst_info(self, dst_path: Text) -> Tuple[Node, Text]:
        if not op.isabs(dst_path):
            if op.basename(dst_path) != dst_path:
                raise ValueError(f'invalid path: {dst_path}')
            # rename only
            return None, dst_path
        else:
            dst_node = await self.get_node_by_path(dst_path)
            if not dst_node:
                # move to the parent folder
                dst_folder, dst_name = op.split(dst_path)
                parent = await self.get_node_by_path(dst_folder)
                return parent, dst_name
            if dst_node.is_file:
                # do not overwrite existing file
                raise FileConflictedError(dst_path)
            # just move to this folder
            return dst_node, None

    async def _inner_rename_node(self,
        node: Node,
        new_parent: Optional[Node],
        name: Optional[Text],
    ) -> Response:
        # sanity check
        if not new_parent and not name:
            raise ValueError('invalid arguments')
        fnbnfpi = self.fetch_node_by_name_from_parent_id
        new_parent_id = node.parent_id if not new_parent else new_parent.id_
        new_name = node.name if not name else name
        new_node = await fnbnfpi(new_name, new_parent_id)
        if new_node:
            raise FileConflictedError(new_node)

        kwargs = {
            'file_id': node.id_,
        }
        if name:
            kwargs['name'] = name
        if new_parent and new_parent.id_ != node.parent_id:
            kwargs['add_parents'] = [new_parent.id_]
            kwargs['remove_parents'] = [node.parent_id]

        rv = await self._client.files.update(**kwargs)
        return rv


class ReadableFile(object):

    def __init__(self, download: Any, node: Node) -> None:
        self._download = download
        self._node = node
        self._offset = 0
        self._response = None
        self._rsps = None

    async def __aenter__(self) -> 'ReadableFile':
        return self

    async def __aexit__(self, type_, exc, tb) -> bool:
        await self._close_response()

    async def __aiter__(self) -> bytes:
        await self._open_response()
        async for chunk in self._response.chunks():
            yield chunk

    async def read(self, length: int) -> bytes:
        await self._open_response()
        return await self._response.read(length)

    async def seek(self, offset: int) -> None:
        self._offset = offset
        await self._close_response()
        await self._open_response()

    async def _download_from_offset(self) -> 'aiohttp.StreamResponse':
        return await self._download(file_id=self._node.id_,
                                    range_=(self._offset, self._node.size))

    async def _open_response(self) -> None:
        if not self._response:
            async with cl.AsyncExitStack() as stack:
                self._response = await stack.enter_async_context(
                    await self._download_from_offset())
                self._rsps = stack.pop_all()

    async def _close_response(self) -> None:
        if self._response:
            await self._rsps.aclose()
            self._response = None
            self._rsps = None


class WritableFile(object):

    def __init__(self,
        initiate: Any,
        upload: Any,
        get_status: Any,
        touch: Any,
        parent_id: Text,
        name: Text,
        timeout: float,
        size: int = None,
        mime_type: Text = None,
    ) -> None:
        self._initiate = initiate
        self._upload = upload
        self._get_status = get_status
        self._touch = touch
        self._parent_id = parent_id
        self._name = name
        self._timeout = timeout
        self._size = size
        self._mime_type = mime_type
        self._url = None
        self._offset = None
        self._queue = asyncio.Queue(maxsize=1)
        self._bg = None
        self._rv = None

    async def __aenter__(self) -> 'WritableFile':
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
        rv = await self._bg
        self._rv = rv.json
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

    @property
    def id_(self) -> Text:
        return self._rv['id']

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

    @cl.asynccontextmanager
    async def _get_one_chunk(self) -> AsyncGenerator[bytes, None]:
        chunk = await self._queue.get()
        try:
            yield chunk
        finally:
            self._queue.task_done()

    async def _get_session_url(self) -> Text:
        rv = await self._initiate(file_name=self._name,
                                  total_file_size=self._size,
                                  parent_id=self._parent_id,
                                  mime_type=self._mime_type)
        url = rv.get_header('Location')
        return url

    async def _upload_to(self) -> 'aiohttp.ClientResponse':
        try:
            rv = await self._upload(self._url, producer=self._produce,
                                    offset=self._offset,
                                    total_file_size=self._size,
                                    mime_type=self._mime_type)
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
        rv = await self._touch(self._name, self._parent_id, self._mime_type)
        return rv


class DownloadError(GoogleDriveError):

    def __init__(self, message: Text) -> None:
        self._message = message

    def __str__(self) -> Text:
        return self._message


class UploadError(GoogleDriveError):

    def __init__(self, message: Text) -> None:
        self._message = message

    def __str__(self) -> Text:
        return self._message


class RemoteNodeError(GoogleDriveError):

    pass


class FileConflictedError(RemoteNodeError):

    def __init__(self, node: Node) -> None:
        self._node = node

    def __str__(self) -> Text:
        return f'remote file already exists: {self._node.name}'

    @property
    def node(self) -> Node:
        return self._node


class InvalidNameError(GoogleDriveError):

    def __init__(self, name: Text) -> None:
        self._name = name

    def __str__(self) -> Text:
        return f'invalid name: {self._name}'


class ParentNotFoundError(RemoteNodeError):

    def __init__(self, id_: Text) -> None:
        self._id = id_

    def __str__(self) -> Text:
        return f'remote parent id not found: {self._id}'


class NodeNotFoundError(RemoteNodeError):

    def __init__(self, path_or_id: Text) -> None:
        self._path_or_id = path_or_id

    def __str__(self) -> Text:
        return f'remote node not found: {self._path_or_id}'


async def download_to_local_by_id(
    drive: Drive,
    node_id: Text,
    path: Text,
) -> Text:
    node = await drive.get_node_by_id(node_id)
    return await download_to_local(drive, node, path)


async def download_to_local(drive: Drive, node: Node, path: Text) -> Text:
    if not op.isdir(path):
        raise ValueError(f'{path} does not exist')

    # check if exists
    complete_path = op.join(path, node.name)
    if op.isfile(complete_path):
        return complete_path

    # exists but not a file
    if op.exists(complete_path):
        raise DownloadError(f'{complete_path} exists but is not a file')

    # if the file is empty, no need to download
    if node.size <= 0:
        open(complete_path, 'w').close()
        return complete_path

    # resume download
    tmp_path = complete_path + '.__tmp__'
    if op.isfile(tmp_path):
        offset = op.getsize(tmp_path)
        if offset > node.size:
            raise DownloadError(
                f'local file size of `{complete_path}` is greater then remote'
                f' ({offset} > {node.size})')
    elif op.exists(tmp_path):
        raise DownloadError(f'{complete_path} exists but is not a file')
    else:
        offset = 0

    if offset < node.size:
        async with await drive.download(node) as fin:
            await fin.seek(offset)
            with open(tmp_path, 'ab') as fout:
                while True:
                    try:
                        async for chunk in fin:
                            fout.write(chunk)
                        break
                    except NetworkError as e:
                        EXCEPTION('wcpan.drive.google', e) << 'download'

                    offset = fout.tell()
                    await fin.seek(offset)

    # rename it back if completed
    os.rename(tmp_path, complete_path)

    return complete_path


async def upload_from_local_by_id(
    drive: Drive,
    parent_id: Text,
    file_path: Text,
    exist_ok: bool = False,
) -> Node:
    node = await drive.get_node_by_id(parent_id)
    return await upload_from_local(drive, node, file_path, exist_ok)


async def upload_from_local(
    drive: Drive,
    parent_node: Node,
    file_path: Text,
    exist_ok: bool = False,
) -> Node:
    # sanity check
    if not op.isfile(file_path):
        raise UploadError('invalid file path')

    file_name = op.basename(file_path)
    total_file_size = op.getsize(file_path)
    mt, _ = mimetypes.guess_type(file_path)

    try:
        fout = await drive.upload(parent_node=parent_node,
                                  file_name=file_name,
                                  file_size=total_file_size,
                                  mime_type=mt)
    except FileConflictedError as e:
        if not exist_ok:
            raise
        return e.node

    async with fout:
        with open(file_path, 'rb') as fin:
            while True:
                try:
                    await upload_feed(fin, fout)
                    break
                except UploadError as e:
                    raise
                except Exception as e:
                    EXCEPTION('wcpan.drive.google', e) << 'upload feed'

                await upload_continue(fin, fout)

    node = await drive.fetch_node_by_id(fout.id_)
    return node


async def upload_feed(fin, fout) -> None:
    while True:
        chunk = fin.read(CHUNK_SIZE)
        if not chunk:
            break
        await fout.write(chunk)


async def upload_continue(fin, fout) -> None:
    offset = await fout.tell()
    await fout.seek(offset)
    fin.seek(offset, os.SEEK_SET)


def transform_changes(
    change_list: List[Dict[Text, Any]],
) -> Generator[Dict[Text, Any], None, None]:
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

        file_ = node_from_api(file_)
        file_ = dict_from_node(file_)
        yield {
            'removed': False,
            'node': file_,
        }
