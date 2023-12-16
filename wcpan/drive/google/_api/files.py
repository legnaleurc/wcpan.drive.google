from typing import Any
from collections.abc import AsyncIterable, AsyncIterator
from contextlib import asynccontextmanager
import json
import re

from aiohttp import ClientResponse
from wcpan.drive.core.types import MediaInfo

from .._network import Network, QueryDict
from .._lib import FOLDER_MIME_TYPE, GoogleFileDict, FILE_FIELDS, utc_now
from ..exceptions import UploadError


_API_ROOT = "https://www.googleapis.com/drive/v3/files"
_UPLOAD_URI = "https://www.googleapis.com/upload/drive/v3/files"


# only for metadata
async def get(
    network: Network,
    file_id: str,
    *,
    fields: str | None = None,
) -> GoogleFileDict:
    query = {}
    if fields is not None:
        query["fields"] = fields

    uri = f"{_API_ROOT}/{file_id}"
    async with network.fetch("GET", uri, query=query) as response:
        rv = await response.json()
    return rv


# https://developers.google.com/drive/api/reference/rest/v3/files/delete
async def delete(network: Network, file_id: str) -> None:
    uri = f"{_API_ROOT}/{file_id}"
    async with network.fetch("DELETE", uri):
        # no body
        pass


async def list_(
    network: Network,
    *,
    corpora: str | None = None,
    drive_id: str | None = None,
    fields: str | None = None,
    order_by: str | None = None,
    page_size: int | None = None,
    page_token: str | None = None,
    q: str | None = None,
    spaces: str | None = None,
) -> dict[str, Any]:
    query: QueryDict = {}
    if corpora is not None:
        query["corpora"] = corpora
    if drive_id is not None:
        query["driveId"] = drive_id
    if fields is not None:
        query["fields"] = fields
    if order_by is not None:
        query["orderBy"] = order_by
    if page_size is not None:
        query["pageSize"] = page_size
    if page_token is not None:
        query["pageToken"] = page_token
    if q is not None:
        query["q"] = q
    if spaces is not None:
        query["spaces"] = spaces

    async with network.fetch("GET", _API_ROOT, query=query) as response:
        rv = await response.json()
    return rv


@asynccontextmanager
async def download(
    network: Network,
    file_id: str,
    range_: tuple[int, int],
    *,
    acknowledge_abuse: bool | None = None,
) -> AsyncIterator[ClientResponse]:
    query: QueryDict = {
        "alt": "media",
    }
    if acknowledge_abuse is not None:
        query["acknowledgeAbuse"] = acknowledge_abuse

    headers = {
        "Range": f"bytes={range_[0]}-{range_[1]}",
    }

    uri = f"{_API_ROOT}/{file_id}"
    async with network.fetch(
        "GET", uri, query=query, headers=headers, timeout=False
    ) as response:
        yield response


# https://developers.google.com/drive/api/guides/manage-uploads#initial-request
async def initiate_uploading(
    network: Network,
    file_name: str,
    total_file_size: int,
    *,
    parent_id: str | None = None,
    mime_type: str | None = None,
    media_info: MediaInfo | None = None,
    app_properties: dict[str, str] | None = None,
) -> str:
    if not file_name:
        raise ValueError("file name is empty")
    if total_file_size <= 0:
        raise ValueError("please use create_empty_file() to create an empty file")

    metadata: dict[str, Any] = {
        "name": file_name,
    }
    if parent_id is not None:
        metadata["parents"] = [parent_id]

    props: dict[str, str] = {}
    if app_properties:
        props.update(app_properties)
    if media_info and media_info.is_image:
        props["image"] = f"{media_info.width} {media_info.height}"
    if media_info and media_info.is_video:
        props[
            "video"
        ] = f"{media_info.width} {media_info.height} {media_info.ms_duration}"
    if props:
        metadata["appProperties"] = props

    body = json.dumps(metadata)
    body = body.encode("utf-8")
    headers = {
        "X-Upload-Content-Length": f"{total_file_size}",
        "Content-Type": "application/json; charset=UTF-8",
        "Content-Length": f"{len(body)}",
    }
    if mime_type is not None:
        headers["X-Upload-Content-Type"] = mime_type

    query: QueryDict = {
        "uploadType": "resumable",
    }

    async with network.fetch(
        "POST", _UPLOAD_URI, query=query, headers=headers, body=body
    ) as response:
        location = response.headers.getone("Location")
    return location


# https://developers.google.com/drive/api/guides/manage-uploads#uploading
async def upload(
    network: Network,
    uri: str,
    producer: AsyncIterable[bytes],
    offset: int,
    total_file_size: int,
    *,
    mime_type: str | None = None,
) -> str:
    if not uri:
        raise ValueError("invalid session URI")
    if not producer:
        raise ValueError("invalid body producer")
    if total_file_size <= 0:
        raise ValueError("please use create_empty_file() to create an empty file")
    if offset < 0 or offset >= total_file_size:
        raise ValueError("offset is out of range")

    last_position = total_file_size - 1
    headers = {
        "Content-Length": f"{total_file_size - offset}",
        "Content-Range": f"bytes {offset}-{last_position}/{total_file_size}",
    }
    if mime_type is not None:
        headers["Content-Type"] = mime_type

    # Producer may raise timeout error, not the upload itself.
    async with network.fetch(
        "PUT", uri, headers=headers, body=producer, timeout=False
    ) as response:
        rv = await response.json()

    return rv["id"]


# https://developers.google.com/drive/api/guides/manage-uploads#resume-upload
async def get_upload_status(
    network: Network,
    uri: str,
    total_file_size: int,
) -> tuple[int, int]:
    """
    Returns:
    (http_status, offset)
    """
    if not uri:
        raise ValueError("invalid session URI")
    if total_file_size <= 0:
        raise ValueError("please use create_empty_file() to create an empty file")

    headers = {
        "Content-Length": "0",
        "Content-Range": f"bytes */{total_file_size}",
    }

    async with network.fetch("PUT", uri, headers=headers) as response:
        status = response.status
        range_ = response.headers.getone("Range", None)

    offset = 0
    # If nothing uploaded yet, the Range header will not present.
    if status == 308 and range_ is not None:
        rv = re.match(r"bytes=0-(\d+)", range_)
        if not rv:
            raise UploadError("invalid upload range")
        offset = int(rv.group(1)) + 1

    return status, offset


async def create_folder(
    network: Network,
    folder_name: str,
    *,
    parent_id: str | None = None,
    app_properties: dict[str, str] | None = None,
) -> GoogleFileDict:
    metadata: dict[str, Any] = {
        "name": folder_name,
        "mimeType": FOLDER_MIME_TYPE,
    }
    if parent_id is not None:
        metadata["parents"] = [parent_id]
    if app_properties:
        metadata["appProperties"] = app_properties
    body = json.dumps(metadata)
    body = body.encode("utf-8")
    headers = {
        "Content-Type": "application/json; charset=UTF-8",
        "Content-Length": f"{len(body)}",
    }

    async with network.fetch("POST", _API_ROOT, headers=headers, body=body) as response:
        rv = await response.json()
    return rv


async def create_empty_file(
    network: Network,
    file_name: str,
    *,
    parent_id: str | None = None,
    mime_type: str | None = None,
    app_properties: dict[str, str] | None = None,
) -> GoogleFileDict:
    if not file_name:
        raise ValueError("file name is empty")

    metadata: dict[str, Any] = {
        "name": file_name,
    }
    if parent_id is not None:
        metadata["parents"] = [parent_id]
    if mime_type is not None:
        metadata["mimeType"] = mime_type
    else:
        metadata["mimeType"] = "application/octet-stream"
    if app_properties:
        metadata["appProperties"] = app_properties
    body = json.dumps(metadata)
    body = body.encode("utf-8")
    headers = {
        "Content-Type": "application/json; charset=UTF-8",
        "Content-Length": f"{len(body)}",
    }

    query: QueryDict = {
        "fields": FILE_FIELDS,
    }

    async with network.fetch(
        "POST", _API_ROOT, query=query, headers=headers, body=body
    ) as response:
        rv = await response.json()
    return rv


async def update(
    network: Network,
    file_id: str,
    *,
    add_parents: list[str] | None = None,
    remove_parents: list[str] | None = None,
    name: str | None = None,
    trashed: bool | None = None,
    app_properties: dict[str, str] | None = None,
    media_info: MediaInfo | None = None,
) -> GoogleFileDict:
    query: QueryDict = {}
    if add_parents is not None:
        query["addParents"] = ",".join(add_parents)
    if remove_parents is not None:
        query["removeParents"] = ",".join(remove_parents)

    metadata = {}
    if name is not None:
        metadata["name"] = name
    if trashed is not None:
        metadata["trashed"] = trashed

    props: dict[str, str] = {}
    if app_properties:
        props.update(app_properties)
    if media_info and media_info.is_image:
        props["image"] = f"{media_info.width} {media_info.height}"
    if media_info and media_info.is_video:
        props[
            "video"
        ] = f"{media_info.width} {media_info.height} {media_info.ms_duration}"
    if props:
        metadata["appProperties"] = props

    if not query and not metadata:
        raise ValueError("not enough parameter")

    now = utc_now()
    metadata["modifiedTime"] = now.isoformat()

    metadata = json.dumps(metadata)
    metadata = metadata.encode("utf-8")
    headers = {
        "Content-Type": "application/json; charset=UTF-8",
        "Content-Length": f"{len(metadata)}",
    }

    uri = f"{_API_ROOT}/{file_id}"
    async with network.fetch(
        "PATCH", uri, query=query, headers=headers, body=metadata
    ) as response:
        rv = await response.json()
    return rv


# https://developers.google.com/drive/api/reference/rest/v3/files/emptyTrash
async def empty_trash(network: Network, *, drive_id: str | None = None) -> None:
    query: QueryDict = {}
    if drive_id is not None:
        query["driveId"] = drive_id

    uri = f"{_API_ROOT}/trash"
    async with network.fetch("DELETE", uri, query=query):
        # no body
        pass
