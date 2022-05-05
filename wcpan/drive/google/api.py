import contextlib
import json

import arrow
from wcpan.drive.core.types import MediaInfo

from .network import Network, Response, ContentProducer
from .util import FOLDER_MIME_TYPE, OAuth2Manager


API_ROOT = 'https://www.googleapis.com/drive/v3'
UPLOAD_URI = 'https://www.googleapis.com/upload/drive/v3/files'


class Client(object):

    def __init__(self, oauth: OAuth2Manager, timeout: int) -> None:
        self._oauth = oauth
        self._timeout = timeout
        self._network = None
        self._api = None
        self._raii = None

    async def __aenter__(self) -> 'Client':
        async with contextlib.AsyncExitStack() as stack:
            self._network = await stack.enter_async_context(
                Network(self._oauth, self._timeout))
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

    async def accept_oauth_code(self, code: str) -> None:
        await self._network.accept_oauth_code(code)

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
        *,
        drive_id: str = None,
        fields: str = None,
    ) -> Response:
        args = {}
        if drive_id is not None:
            args['driveId'] = drive_id
        if fields is not None:
            args['fields'] = fields

        uri = self._root + '/startPageToken'
        rv = await self._network.fetch('GET', uri, args)
        return rv

    async def list_(self,
        page_token: str,
        *,
        drive_id: str = None,
        fields: str = None,
        include_corpus_removals: bool = None,
        include_removed: bool = None,
        page_size: int = None,
        restrict_to_my_drive: bool = None,
        spaces: str = None,
    ) -> Response:
        args = {
            'pageToken': page_token,
        }
        if drive_id is not None:
            args['driveId'] = drive_id
        if fields is not None:
            args['fields'] = fields
        if include_corpus_removals is not None:
            args['includeCorpusRemovals'] = include_corpus_removals
        if include_removed is not None:
            args['includeRemoved'] = include_removed
        if page_size is not None:
            args['pageSize'] = page_size
        if restrict_to_my_drive is not None:
            args['restrictToMyDrive'] = restrict_to_my_drive
        if spaces is not None:
            args['spaces'] = spaces

        rv = await self._network.fetch('GET', self._root, args)
        return rv


class Files(object):

    def __init__(self, network: Network) -> None:
        self._network = network
        self._root = API_ROOT + '/files'

    # only for metadata
    async def get(self,
        file_id: str,
        *,
        fields: str = None,
    ) -> Response:
        args = {}
        if fields is not None:
            args['fields'] = fields

        uri = self._root + '/' + file_id
        rv = await self._network.fetch('GET', uri, args)
        return rv

    async def list_(self,
        *,
        corpora: str = None,
        drive_id: str = None,
        fields: str = None,
        order_by: str = None,
        page_size: int = None,
        page_token: str = None,
        q: str = None,
        spaces: str = None,
    ) -> Response:
        args = {}
        if corpora is not None:
            args['corpora'] = corpora
        if drive_id is not None:
            args['driveId'] = drive_id
        if fields is not None:
            args['fields'] = fields
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

        rv = await self._network.fetch('GET', self._root, args)
        return rv

    async def download(self,
        file_id: str,
        range_: tuple[int, int],
        *,
        acknowledge_abuse: bool = None,
    ) -> Response:
        args = {
            'alt': 'media',
        }
        if acknowledge_abuse is not None:
            args['acknowledgeAbuse'] = acknowledge_abuse

        headers = {
            'Range': f'bytes={range_[0]}-{range_[1]}',
        }

        uri = self._root + '/' + file_id
        rv = await self._network.download('GET', uri, args, headers=headers)
        return rv

    async def initiate_uploading(self,
        file_name: str,
        total_file_size: int,
        *,
        parent_id: str = None,
        mime_type: str = None,
        media_info: MediaInfo = None,
        app_properties: dict[str, str] = None,
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

        props = {}
        if app_properties:
            props.update(app_properties)
        if media_info and media_info.is_image:
            props['image'] = f'{media_info.width} {media_info.height}'
        if media_info and media_info.is_video:
            props['video'] = f'{media_info.width} {media_info.height} {media_info.ms_duration}'
        if props:
            metadata['appProperties'] = props

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

        rv = await self._network.fetch('POST', UPLOAD_URI, args,
                                       headers=headers, body=metadata)
        return rv

    async def upload(self,
        uri: str,
        producer: ContentProducer,
        offset: int,
        total_file_size: int,
        *,
        mime_type: str = None,
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
        uri: str,
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
        folder_name: str,
        *,
        parent_id: str = None,
        app_properties: dict[str, str] = None,
    ) -> Response:
        metadata = {
            'name': folder_name,
            'mimeType': FOLDER_MIME_TYPE,
        }
        if parent_id is not None:
            metadata['parents'] = [parent_id]
        if app_properties:
            metadata['appProperties'] = app_properties
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
        file_name: str,
        *,
        parent_id: str = None,
        mime_type: str = None,
        app_properties: dict[str, str] = None,
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
        if app_properties:
            metadata['appProperties'] = app_properties
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
        file_id: str,
        *,
        add_parents: list[str] = None,
        remove_parents: list[str] = None,
        name: str = None,
        trashed: bool = None,
        app_properties: list[str, str] = None,
        media_info: MediaInfo = None,
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

        props = {}
        if app_properties:
            props.update(app_properties)
        if media_info and media_info.is_image:
            props['image'] = f'{media_info.width} {media_info.height}'
        if media_info and media_info.is_video:
            props['video'] = f'{media_info.width} {media_info.height} {media_info.ms_duration}'
        if props:
            metadata['appProperties'] = props

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
