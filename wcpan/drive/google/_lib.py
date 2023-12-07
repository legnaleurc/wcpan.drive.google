from collections.abc import Iterable
from datetime import datetime, UTC
from typing import TypedDict, NotRequired, Literal

from wcpan.drive.core.types import Node, ChangeAction


FOLDER_MIME_TYPE = "application/vnd.google-apps.folder"
FILE_FIELDS = ",".join(
    [
        "id",
        "name",
        "mimeType",
        "trashed",
        "parents",
        "createdTime",
        "modifiedTime",
        "md5Checksum",
        "size",
        "shared",
        "ownedByMe",
        "imageMediaMetadata/width",
        "imageMediaMetadata/height",
        "videoMediaMetadata/width",
        "videoMediaMetadata/height",
        "videoMediaMetadata/durationMillis",
        "appProperties",
    ]
)
CHANGE_FIELDS = ",".join(
    [
        "nextPageToken",
        "newStartPageToken",
        f"changes(changeType,fileId,removed,file({FILE_FIELDS}))",
    ]
)
LIST_FIELDS = ",".join(
    [
        "nextPageToken",
        f"files({FILE_FIELDS})",
    ]
)


class GoogleImageDict(TypedDict):
    width: int
    height: int


class GoogleVideoDict(GoogleImageDict):
    durationMillis: str


class GoogleFileDict(TypedDict):
    id: str
    name: str
    trashed: bool
    createdTime: str
    modifiedTime: str
    mimeType: str
    parents: list[str]
    size: NotRequired[str]
    md5Checksum: NotRequired[str]
    appProperties: NotRequired[dict[str, str]]
    imageMediaMetadata: NotRequired[GoogleImageDict]
    videoMediaMetadata: NotRequired[GoogleVideoDict]
    shared: bool
    ownedByMe: bool


class GoogleChangeDict(TypedDict):
    changeType: Literal["file", "drive"]
    removed: bool
    fileId: str
    file: GoogleFileDict


def reverse_cliend_id(client_id: str) -> str:
    parts = client_id.split(".")
    parts = parts[::-1]
    return ".".join(parts)


def node_from_api(data: GoogleFileDict) -> Node:
    id_ = data["id"]

    parent_list = data["parents"]
    parent_id = None if not parent_list else parent_list[0]

    is_folder = data["mimeType"] == FOLDER_MIME_TYPE

    ctime = datetime.fromisoformat(data["createdTime"])
    mtime = datetime.fromisoformat(data["modifiedTime"])

    size = int(data.get("size", "0"))
    hash_ = data.get("md5Checksum", "")

    private = data.get("appProperties", None)

    is_image = False
    is_video = False
    width = 0
    height = 0
    ms_duration = 0

    if private and "image" in private:
        w, h = private["image"].split(" ")
        is_image = True
        width = int(w)
        height = int(h)
        del private["image"]
    if not is_image and "imageMediaMetadata" in data:
        is_image = True
        width = data["imageMediaMetadata"]["width"]
        height = data["imageMediaMetadata"]["height"]

    if private and "video" in private:
        w, h, d = private["video"].split(" ")
        is_video = True
        width = int(w)
        height = int(h)
        ms_duration = int(d)
        del private["video"]
    if not is_video and "videoMediaMetadata" in data:
        is_video = True
        width = int(data["videoMediaMetadata"]["width"])
        height = int(data["videoMediaMetadata"]["height"])
        ms_duration = int(data["videoMediaMetadata"]["durationMillis"])

    return Node(
        id=id_,
        name=data["name"],
        is_trashed=data["trashed"],
        ctime=ctime,
        mtime=mtime,
        parent_id=parent_id,
        is_directory=is_folder,
        mime_type="" if is_folder else data["mimeType"],
        hash=hash_,
        size=size,
        is_image=is_image,
        is_video=is_video,
        width=width,
        height=height,
        ms_duration=ms_duration,
        private=private,
    )


def normalize_changes(
    change_list: list[GoogleChangeDict],
) -> Iterable[ChangeAction]:
    for change in change_list:
        if change["changeType"] != "file":
            continue

        is_removed = change["removed"]
        if is_removed:
            yield True, change["fileId"]
            continue

        file_ = change["file"]
        is_shared = file_["shared"]
        is_owned_by_me = file_["ownedByMe"]
        if is_shared or not is_owned_by_me:
            continue

        node = node_from_api(file_)
        yield False, node


def utc_now() -> datetime:
    return datetime.now(UTC)
