from logging import getLogger
import re

from aiohttp import ClientResponseError
from wcpan.drive.core.exceptions import NodeNotFoundError
from wcpan.drive.core.types import Node

from ._lib import FILE_FIELDS, GoogleFileDict, LIST_FIELDS, node_from_api
from ._network import Network


async def fetch_child_by_name(
    network: Network,
    name: str,
    parent_id: str,
) -> Node | None:
    from ._api.files import list_

    safe_name = re.sub(r"[\\']", r"\\\g<0>", name)
    query = " and ".join(
        [
            f"'{parent_id}' in parents",
            f"name = '{safe_name}'",
            f"trashed = false",
        ]
    )
    fields = f"files({FILE_FIELDS})"
    try:
        rv = await list_(network, q=query, fields=fields)
    except ClientResponseError as e:
        if e.status == 400:
            getLogger(__name__).debug(f"invalid query string: {query}")
            raise ValueError(name) from e
        if e.status == 404:
            raise NodeNotFoundError(parent_id) from e
        raise

    files = rv["files"]
    if not files:
        return None

    node = node_from_api(files[0])
    return node


async def fetch_node_by_id(network: Network, node_id: str) -> Node:
    from ._api.files import get

    try:
        rv = await get(network, node_id, fields=FILE_FIELDS)
    except ClientResponseError as e:
        if e.status == 404:
            raise NodeNotFoundError(node_id) from e
        raise

    node = node_from_api(rv)
    return node


async def fetch_children(network: Network, parent_id: str) -> list[Node]:
    from ._api.files import list_

    query = " and ".join(
        [
            f"'{parent_id}' in parents",
        ]
    )
    page_token = None
    files: list[GoogleFileDict] = []
    try:
        while True:
            rv = await list_(
                network,
                q=query,
                fields=LIST_FIELDS,
                page_size=1000,
                page_token=page_token,
            )
            files.extend(rv["files"])
            page_token = rv.get("nextPageToken", None)
            if not page_token:
                break
    except ClientResponseError as e:
        if e.status == 404:
            raise NodeNotFoundError(parent_id) from e
        raise

    node_list = [node_from_api(_) for _ in files]
    return node_list
