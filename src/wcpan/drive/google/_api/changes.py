from typing import Any

from .._network import Network, QueryDict


_API_ROOT = "https://www.googleapis.com/drive/v3/changes"


async def get_start_page_token(
    network: Network,
    *,
    drive_id: str | None = None,
    fields: str | None = None,
) -> dict[str, Any]:
    query: QueryDict = {}
    if drive_id is not None:
        query["driveId"] = drive_id
    if fields is not None:
        query["fields"] = fields

    uri = _API_ROOT + "/startPageToken"
    async with network.fetch("GET", uri, query=query) as response:
        rv = await response.json()
    return rv


async def list_(
    network: Network,
    page_token: str,
    *,
    drive_id: str | None = None,
    fields: str | None = None,
    include_corpus_removals: bool | None = None,
    include_removed: bool | None = None,
    page_size: int | None = None,
    restrict_to_my_drive: bool | None = None,
    spaces: str | None = None,
) -> dict[str, Any]:
    query: QueryDict = {
        "pageToken": page_token,
    }
    if drive_id is not None:
        query["driveId"] = drive_id
    if fields is not None:
        query["fields"] = fields
    if include_corpus_removals is not None:
        query["includeCorpusRemovals"] = include_corpus_removals
    if include_removed is not None:
        query["includeRemoved"] = include_removed
    if page_size is not None:
        query["pageSize"] = page_size
    if restrict_to_my_drive is not None:
        query["restrictToMyDrive"] = restrict_to_my_drive
    if spaces is not None:
        query["spaces"] = spaces

    async with network.fetch("GET", _API_ROOT, query=query) as response:
        rv = await response.json()
    return rv
