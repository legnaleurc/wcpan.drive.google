import contextlib as cl
import json
from typing import List, Text, Tuple

import arrow

from .network import Network, Response, ContentProducer
from .util import FOLDER_MIME_TYPE, Settings


API_ROOT = 'https://www.googleapis.com/drive/v3'


class Client(object):

    def __init__(self, settings: Settings, timeout: int) -> None:
        self._settings = settings
        self._timeout = timeout
        self._network = None
        self._api = None
        self._raii = None

    async def __aenter__(self) -> 'Client':
        async with cl.AsyncExitStack() as stack:
            self._network = await stack.enter_async_context(
                Network(self._settings, self._timeout))
            self._api = {
                'changes': Changes(self._network),
                'files': Files(self._network),
            }
            self._raii = stack.pop_all()

        return self

    async def __aexit__(self, type_, value, traceback) -> bool:
        await self._raii.aclose()
        self._api = None
        self._network = None
        self._raii = None

    @property
    def changes(self) -> 'Changes':
        return self._api['changes']

    @property
    def files(self) -> 'Files':
        return self._api['files']


class Changes(object):

    def __init__(self, network: Network) -> None:
        self._network = network
        self._root = API_ROOT + '/changes'

    async def get_start_page_token(self,
        supports_team_drives: bool = None,
        team_drive_id: Text = None,
    ) -> Response:
        args = {}
        if supports_team_drives is not None:
            args['supportsTeamDrives'] = supports_team_drives
        if team_drive_id is not None:
            args['teamDriveId'] = team_drive_id

        uri = self._root + '/startPageToken'
        rv = await self._network.fetch('GET', uri, args)
        return rv

    async def list_(self,
        page_token: Text,
        include_corpus_removals: bool = None,
        include_removed: bool = None,
        include_team_drive_items: bool = None,
        page_size: int = None,
        restrict_to_my_drive: bool = None,
        spaces: Text = None,
        supports_team_drives: bool = None,
        team_drive_id: Text = None,
        fields: Text = None,
    ) -> Response:
        args = {
            'pageToken': page_token,
        }
        if include_corpus_removals is not None:
            args['includeCorpusRemovals'] = include_corpus_removals
        if include_removed is not None:
            args['includeRemoved'] = include_removed
        if include_team_drive_items is not None:
            args['includeTeamDriveItems'] = include_team_drive_items
        if page_size is not None:
            args['pageSize'] = page_size
        if restrict_to_my_drive is not None:
            args['restrictToMyDrive'] = restrict_to_my_drive
        if spaces is not None:
            args['spaces'] = spaces
        if supports_team_drives is not None:
            args['supportsTeamDrives'] = supports_team_drives
        if team_drive_id is not None:
            args['teamDriveId'] = team_drive_id
        if fields is not None:
            args['fields'] = fields

        rv = await self._network.fetch('GET', self._root, args)
        return rv


class Files(object):

    def __init__(self, network: Network) -> None:
        self._network = network
        self._root = API_ROOT + '/files'
        self._upload_uri = 'https://www.googleapis.com/upload/drive/v3/files'

    # only for metadata
    async def get(self,
        file_id: Text,
        supports_team_drives: bool = None,
        fields: Text = None,
    ) -> Response:
        args = {}
        if supports_team_drives is not None:
            args['supportsTeamDrives'] = supports_team_drives
        if fields is not None:
            args['fields'] = fields

        uri = self._root + '/' + file_id
        rv = await self._network.fetch('GET', uri, args)
        return rv

    async def list_(self,
        corpora: Text = None,
        corpus: Text = None,
        include_team_drive_items: bool = None,
        order_by: Text = None,
        page_size: int = None,
        page_token: Text = None,
        q: Text = None,
        spaces: Text = None,
        supports_team_drives: bool = None,
        team_drive_id: Text = None,
        fields: Text = None,
    ) -> Response:
        args = {}
        if corpora is not None:
            args['corpora'] = corpora
        if corpus is not None:
            args['corpus'] = corpus
        if include_team_drive_items is not None:
            args['includeTeamDriveItems'] = include_team_drive_items
        if order_by is not None:
            args['orderBy'] = order_by
        if page_size is not None:
            args['pageSize'] = page_size
        if page_token is not None:
            args['pageToken'] = page_token
        if q is not None:
            args['q'] = q
        if spaces is not None:
            args['spaces'] = spaces
        if supports_team_drives is not None:
            args['supportsTeamDrives'] = supports_team_drives
        if team_drive_id is not None:
            args['teamDriveId'] = team_drive_id
        if fields is not None:
            args['fields'] = fields

        rv = await self._network.fetch('GET', self._root, args)
        return rv

    async def download(self,
        file_id: Text,
        range_: Tuple[int, int],
        acknowledge_abuse: bool = None,
        supports_team_drives: bool = None,
    ) -> Response:
        args = {
            'alt': 'media',
        }
        if acknowledge_abuse is not None:
            args['acknowledgeAbuse'] = acknowledge_abuse
        if supports_team_drives is not None:
            args['supportsTeamDrives'] = supports_team_drives

        headers = {
            'Range': f'bytes={range_[0]}-{range_[1]}',
        }

        uri = self._root + '/' + file_id
        rv = await self._network.download('GET', uri, args, headers=headers)
        return rv

    async def initiate_uploading(self,
        file_name: Text,
        total_file_size: int,
        parent_id: Text = None,
        mime_type: Text = None,
    ) -> Response:
        if not file_name:
            raise ValueError('file name is empty')
        if total_file_size <= 0:
            raise ValueError('please use create_empty_file() to create an empty file')

        metadata = {
            'name': file_name,
        }
        if parent_id is not None:
            metadata['parents'] = [parent_id]
        metadata = json.dumps(metadata)
        metadata = metadata.encode('utf-8')
        headers = {
            'X-Upload-Content-Length': total_file_size,
            'Content-Type': 'application/json; charset=UTF-8',
            'Content-Length': len(metadata),
        }
        if mime_type is not None:
            headers['X-Upload-Content-Type'] = mime_type

        args = {
            'uploadType': 'resumable',
        }

        rv = await self._network.fetch('POST', self._upload_uri, args,
                                       headers=headers, body=metadata)
        return rv

    async def upload(self,
        uri: Text,
        producer: ContentProducer,
        offset: int,
        total_file_size: int,
        mime_type: Text = None,
    ) -> Response:
        if not uri:
            raise ValueError('invalid session URI')
        if not producer:
            raise ValueError('invalid body producer')
        if total_file_size <= 0:
            raise ValueError('please use create_empty_file() to create an empty file')
        if offset < 0 or offset >= total_file_size:
            raise ValueError('offset is out of range')

        last_position = total_file_size - 1
        headers = {
            'Content-Length': total_file_size - offset,
            'Content-Range': f'bytes {offset}-{last_position}/{total_file_size}',
        }
        if mime_type is not None:
            headers['Content-Type'] = mime_type

        # Producer may raise timeout error, not the upload itself.
        rv = await self._network.upload('PUT', uri, headers=headers,
                                        body=producer)
        return rv

    async def get_upload_status(self,
        uri: Text,
        total_file_size: int,
    ) -> Response:
        if not uri:
            raise ValueError('invalid session URI')
        if total_file_size <= 0:
            raise ValueError('please use create_empty_file() to create an empty file')

        headers = {
            'Content-Length': 0,
            'Content-Range': f'bytes */{total_file_size}',
        }
        rv = await self._network.fetch('PUT', uri, headers=headers)
        return rv

    async def create_folder(self,
        folder_name: Text,
        parent_id: Text = None,
    ) -> Response:
        metadata = {
            'name': folder_name,
            'mimeType': FOLDER_MIME_TYPE,
        }
        if parent_id is not None:
            metadata['parents'] = [parent_id]
        metadata = json.dumps(metadata)
        metadata = metadata.encode('utf-8')
        headers = {
            'Content-Type': 'application/json; charset=UTF-8',
            'Content-Length': len(metadata),
        }

        rv = await self._network.fetch('POST', self._root, headers=headers,
                                       body=metadata)
        return rv

    async def create_empty_file(self,
        file_name: Text,
        parent_id: Text = None,
        mime_type: Text = None,
    ) -> Response:
        if not file_name:
            raise ValueError('file name is empty')

        metadata = {
            'name': file_name,
        }
        if parent_id is not None:
            metadata['parents'] = [parent_id]
        if mime_type is not None:
            metadata['mimeType'] = mime_type
        else:
            metadata['mimeType'] = 'application/octet-stream'
        metadata = json.dumps(metadata)
        metadata = metadata.encode('utf-8')
        headers = {
            'Content-Type': 'application/json; charset=UTF-8',
            'Content-Length': len(metadata),
        }

        rv = await self._network.fetch('POST', self._root, headers=headers,
                                       body=metadata)
        return rv

    async def update(self,
        file_id: Text,
        name: Text = None,
        add_parents: List[Text] = None,
        remove_parents: List[Text] = None,
        trashed: bool = None,
    ) -> Response:
        args = {}
        if add_parents is not None:
            args['addParents'] = ','.join(add_parents)
        if remove_parents is not None:
            args['removeParents'] = ','.join(remove_parents)

        metadata = {}
        if name is not None:
            metadata['name'] = name
        if trashed is not None:
            metadata['trashed'] = trashed

        if not args and not metadata:
            raise ValueError('not enough parameter')

        now = arrow.utcnow()
        metadata['modifiedTime'] = now.isoformat()

        metadata = json.dumps(metadata)
        metadata = metadata.encode('utf-8')
        headers = {
            'Content-Type': 'application/json; charset=UTF-8',
            'Content-Length': len(metadata),
        }

        uri = self._root + '/' + file_id
        rv = await self._network.fetch('PATCH', uri, args, headers=headers,
                                       body=metadata)
        return rv
