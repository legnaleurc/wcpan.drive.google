import contextlib as cl
import functools as ft
import hashlib as hl
import mimetypes
import os
import os.path as op
import re
from typing import (Any, AsyncGenerator, Awaitable, Dict, List, Optional, Text,
                    Tuple, Union)

from wcpan.logger import INFO, WARNING, DEBUG

from .api import Client
from .cache import Cache, Node
from .network import ContentProducer, NetworkError, Response
from .util import (Settings, GoogleDriveError, stream_md5sum, FOLDER_MIME_TYPE,
                   CHUNK_SIZE)


FILE_FIELDS = 'id,name,mimeType,trashed,parents,createdTime,modifiedTime,md5Checksum,size'
CHANGE_FIELDS = 'nextPageToken,newStartPageToken,changes(fileId,removed,file({0}))'.format(FILE_FIELDS)
EMPTY_MD5SUM = 'd41d8cd98f00b204e9800998ecf8427e'


class Drive(object):

    def __init__(self, settings_path: Text = None) -> None:
        self._settings = Settings(settings_path)
        self._client = None
        self._db = None
        self._raii = None

    async def __aenter__(self) -> 'Drive':
        async with cl.AsyncExitStack() as stack:
            self._client = await stack.enter_async_context(
                Client(self._settings))
            dsn = self._settings['nodes_database_file']
            self._db = await stack.enter_async_context(Cache(dsn))
            self._raii = stack.pop_all()
        return self

    async def __aexit__(self, exc_type, exc, tb) -> bool:
        await self._raii.aclose()
        self._client = None
        self._db = None
        self._raii = None

    async def sync(self) -> bool:
        INFO('wcpan.drive.google') << 'sync begin'

        try:
            check_point = await self._db.get_metadata('check_point')
        except KeyError:
            check_point = '1'

        # first time, get root node
        if check_point == '1':
            rv = await self._client.files.get('root', fields=FILE_FIELDS)
            rv = await rv.json()
            rv['name'] = None
            rv['parents'] = []
            node = Node.from_api(rv)
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
            rv = await rv.json()
            next_page_token = rv.get('nextPageToken', None)
            new_start_page_token = rv.get('newStartPageToken', None)
            changes = rv['changes']

            check_point = next_page_token if next_page_token is not None else new_start_page_token

            await self._db.apply_changes(changes, check_point)
            changes_list_args['page_token'] = check_point

            INFO('wcpan.drive.google') << 'applied' << len(changes) << 'changes'

        INFO('wcpan.drive.google') << 'sync end'
        return True

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

    async def download_file_by_id(self, node_id: Text, path: Text) -> bool:
        node = await self.get_node_by_id(node_id)
        return await self.download_file(node, path)

    async def download_file(self, node: Node, path: Text) -> bool:
        # sanity check
        if not node:
            raise ValueError('node is none')
        if node.is_folder:
            raise ValueError('node should be a file')
        if not op.isdir(path):
            raise ValueError('{0} does not exist'.format(path))

        # check if exists
        complete_path = op.join(path, node.name)
        if op.isfile(complete_path):
            return True

        # exists but not a file
        if op.exists(complete_path):
            msg = '{0} exists but is not a file'.format(complete_path)
            raise DownloadError(msg)

        # if the file is empty, no need to download
        if node.size <= 0:
            open(complete_path, 'w').close()
            return True

        # resume download
        tmp_path = complete_path + '.__tmp__'
        if op.isfile(tmp_path):
            offset = op.getsize(tmp_path)
            if offset > node.size:
                msg = ('local file size of `{0}` is greater then remote ({1} > {2})'
                       .format(complete_path, offset, node.size))
                raise DownloadError(msg)
        elif op.exists(tmp_path):
            msg = '{0} exists but is not a file'.format(complete_path)
            raise DownloadError(msg)
        else:
            offset = 0
        range_ = (offset, node.size)

        with open(tmp_path, 'ab') as fout:
            api = self._client.files
            rv = await api.download(file_id=node.id_, range_=range_)
            async for chunk in rv.chunks():
                fout.write(chunk)

        # rename it back if completed
        os.rename(tmp_path, complete_path)

        return True

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
        rv = await rv.json()
        node = await self.fetch_node_by_id(rv['id'])

        return node

    async def upload_file(self,
        file_path: Text,
        parent_node: Node,
        exist_ok: bool = False,
    ) -> Node:
        # sanity check
        if not parent_node:
            raise UploadError('invalid parent node')
        if not parent_node.is_folder:
            raise UploadError('invalid parent node')
        if not op.isfile(file_path):
            raise UploadError('invalid file path')

        api = self._client.files
        file_name = op.basename(file_path)

        # do not upload if remote exists a same file
        node = await self.fetch_node_by_name_from_parent_id(file_name,
                                                            parent_node.id_)
        if node:
            if exist_ok:
                INFO('wcpan.drive.google') << 'skipped (existing)' << file_path
                return node
            else:
                raise FileConflictedError(node)

        total_file_size = op.getsize(file_path)
        mt, e = mimetypes.guess_type(file_path)
        if total_file_size <= 0:
            rv = await api.create_empty_file(file_name=file_name,
                                             parent_id=parent_node.id_,
                                             mime_type=mt)
            local_md5 = EMPTY_MD5SUM
        else:
            args = {
                'file_path': file_path,
                'file_name': file_name,
                'total_file_size': total_file_size,
                'parent_id': parent_node.id_,
                'mime_type': mt,
            }
            rv, local_md5 = await self._inner_upload_file(**args)

        rv = await rv.json()
        node = await self.fetch_node_by_id(rv['id'])

        if node.md5 != local_md5:
            raise UploadError('md5 mismatch')

        return node

    async def fetch_node_by_name_from_parent_id(self,
        name: Text,
        parent_id: Text,
    ) -> Node:
        safe_name = re.sub(r"[\\']", r"\\\g<0>", name)
        query = "'{0}' in parents and name = '{1}'".format(parent_id,
                                                           safe_name)
        fields = 'files({0})'.format(FILE_FIELDS)
        while True:
            try:
                rv = await self._client.files.list_(q=query, fields=fields)
                break
            except NetworkError as e:
                if e.status == '400':
                    DEBUG('wcpan.drive.google') << 'failed query:' << query
                if e.fatal:
                    raise

        rv = await rv.json()
        files = rv['files']
        if not files:
            return None

        node = Node.from_api(files[0])
        await self._db.insert_node(node)
        return node

    async def fetch_node_by_id(self, node_id: Text) -> Node:
        rv = await self._client.files.get(node_id, fields=FILE_FIELDS)
        rv = await rv.json()
        node = Node.from_api(rv)
        await self._db.insert_node(node)
        return node

    async def trash_node_by_id(self, node_id: Text) -> Node:
        node = await self.get_root_node()
        if node_id == node.id_:
            return
        await self._client.files.update(node_id, trashed=True)

        node = await self.get_node_by_id(node_id)
        node.trashed = True
        await self._db.insert_node(node)

        # update all children
        async for parent, folders, files in drive_walk(self, node):
            for folder in folders:
                folder.trashed = True
                await self._db.insert_node(folder)
            for f in files:
                f.trashed = True
                await self._db.insert_node(f)

        return node

    async def trash_node(self, node: Node) -> Node:
        return await self.trash_node_by_id(node.id_)

    async def rename_node_by_path(self, src_path: Text, dst_path: Text) -> Node:
        node = await self.get_node_by_path(src_path)
        if not node:
            raise FileNotFoundError(src_path)
        return await self.rename_node(node, dst_path)

    async def rename_node_by_id(self, node_id: Text, dst_path: Text) -> Node:
        # TODO raise exception for invalid nodes
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
        parent, dst_name = await self._get_dst_info(dst_path)
        await self._inner_rename_node(src_node, parent, dst_name)

        # update local cache
        node = await self.fetch_node_by_id(src_node.id_)
        await self._db.insert_node(node)
        return node

    async def _get_dst_info(self, dst_path: Text) -> Tuple[Node, Text]:
        if not op.isabs(dst_path):
            if op.basename(dst_path) != dst_path:
                raise ValueError('invalid path: {0}'.format(dst_path))
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

    async def _inner_upload_file(self,
        file_path: Text,
        file_name: Text,
        total_file_size: int,
        parent_id: Text,
        mime_type: Text,
    ) -> Tuple[Response, Text]:
        api = self._client.files

        rv = await api.initiate_uploading(file_name=file_name,
                                          total_file_size=total_file_size,
                                          parent_id=parent_id,
                                          mime_type=mime_type)

        url = rv.get_header('Location')

        with open(file_path, 'rb') as fin:
            hasher = hl.md5()
            reader = ft.partial(file_producer, fin, hasher)
            uploader = ft.partial(self._inner_try_upload_file,
                                  url=url, producer=reader,
                                  total_file_size=total_file_size,
                                  mime_type=mime_type)

            retried = False
            offset = 0
            while True:
                ok, rv = await uploader(offset=offset)
                if ok:
                    break
                offset = rv
                fin.seek(offset, os.SEEK_SET)
                retried = True

            if retried:
                fin.seek(0, os.SEEK_SET)
                local_md5 = stream_md5sum(fin)
            else:
                local_md5 = hasher.hexdigest()

        return rv, local_md5

    async def _inner_try_upload_file(self,
        url: Text,
        producer: ContentProducer,
        offset: int,
        total_file_size: int,
        mime_type: Text,
    ) -> Tuple[bool, Union[Response, int]]:
        api = self._client.files

        try:
            rv = await api.upload(url, producer=producer, offset=offset,
                                  total_file_size=total_file_size,
                                  mime_type=mime_type)
            return True, rv
        except NetworkError as e:
            if e.status == '404':
                raise UploadError('the upload session has been expired')
            if e.fatal:
                raise

        while True:
            try:
                rv = await api.get_upload_status(url, total_file_size)
                break
            except NetworkError as e:
                if e.status == '410':
                    # This means the temporary URL has been cleaned up by Google
                    # Drive, so the client has to start over again.
                    msg = (
                        'the uploaded resource is gone, '
                        'code: "{0}", reason: "{1}".'
                    ).format(e.json['code'], e.json['message'])
                    raise UploadError(msg)
                if e.fatal:
                    raise

        if rv.status != '308':
            raise UploadError('invalid upload status')
        rv = rv.get_header('Range')
        if not rv:
            raise UploadError('invalid upload status')
        rv = re.match(r'bytes=(\d+)-(\d+)', rv)
        if not rv:
            raise UploadError('invalid upload status')
        rv = int(rv.group(2))

        return False, rv

    async def _inner_rename_node(self,
        node: Node,
        new_parent: Optional[Node],
        name: Optional[Text],
    ) -> Response:
        if not new_parent and not name:
            raise ValueError('invalid arguments')

        kwargs = {
            'file_id': node.id_,
        }
        if name:
            kwargs['name'] = name
        if new_parent and new_parent.id_ != node.parent_id:
            kwargs['add_parents'] = [new_parent.id_]
            kwargs['remove_parents'] = [node.parent_id]

        while True:
            try:
                rv = await self._client.files.update(**kwargs)
                break
            except NetworkError as e:
                if e.fatal:
                    raise
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


class FileConflictedError(GoogleDriveError):

    def __init__(self, node: Node) -> None:
        self._node = node

    def __str__(self) -> Text:
        return 'remote file already exists: ' + self._node.name


async def file_producer(
    fin: 'file',
    hasher: 'hashlib.hash',
) -> AsyncGenerator[bytes, None]:
    while True:
        chunk = fin.read(CHUNK_SIZE)
        if not chunk:
            break
        hasher.update(chunk)
        yield chunk


async def drive_walk(drive, node):
    if not node.is_folder:
        return
    q = [node]
    while q:
        node = q[0]
        del q[0]
        children = await drive.get_children(node)
        folders = list(filter(lambda _: _.is_folder, children))
        files = list(filter(lambda _: _.is_file, children))
        yield node, folders, files
        q.extend(folders)
