import asyncio
import contextlib as cl
import concurrent.futures as cf
import functools as ft
import pathlib as pl
import re
import sqlite3
from typing import Any, Dict, List, Optional, Text, Union

import arrow

from . import util as u


SQL_CREATE_TABLES = [
    '''
    CREATE TABLE metadata (
        key TEXT NOT NULL,
        value TEXT,
        PRIMARY KEY (key)
    );
    ''',

    '''
    CREATE TABLE nodes (
        id TEXT NOT NULL,
        name TEXT,
        trashed BOOLEAN,
        created INTEGER,
        modified INTEGER,
        PRIMARY KEY (id)
    );
    ''',
    'CREATE INDEX ix_nodes_names ON nodes(name);',
    'CREATE INDEX ix_nodes_trashed ON nodes(trashed);',
    'CREATE INDEX ix_nodes_created ON nodes(created);',
    'CREATE INDEX ix_nodes_modified ON nodes(modified);',

    '''
    CREATE TABLE files (
        id TEXT NOT NULL,
        mime_type TEXT,
        md5 TEXT,
        size INTEGER,
        PRIMARY KEY (id),
        FOREIGN KEY (id) REFERENCES nodes (id)
    );
    ''',
    'CREATE INDEX ix_files_mime_type ON files(mime_type);',

    '''
    CREATE TABLE parentage (
        parent TEXT NOT NULL,
        child TEXT NOT NULL,
        PRIMARY KEY (parent, child),
        FOREIGN KEY (parent) REFERENCES nodes (id),
        FOREIGN KEY (child) REFERENCES nodes (id)
    );
    ''',
    'CREATE INDEX ix_parentage_parent ON parentage(parent);',
    'CREATE INDEX ix_parentage_child ON parentage(child);',

    '''
    CREATE TABLE images (
        id TEXT NOT NULL,
        width INTEGER NOT NULL,
        height INTEGER NOT NULL,
        PRIMARY KEY (id),
        FOREIGN KEY (id) REFERENCES nodes (id)
    );
    ''',

    '''
    CREATE TABLE videos (
        id TEXT NOT NULL,
        width INTEGER NOT NULL,
        height INTEGER NOT NULL,
        ms_duration INTEGER NOT NULL,
        PRIMARY KEY (id),
        FOREIGN KEY (id) REFERENCES nodes (id)
    );
    ''',

    'PRAGMA user_version = 3;',
]


CURRENT_SCHEMA_VERSION = 3


# NOTE *MUST* be picklable
class CacheError(u.GoogleDriveError):

    def __init__(self, message: Text) -> None:
        super(CacheError, self).__init__()

        self._message = message

    def __str__(self) -> Text:
        return self._message

    def __reduce__(self):
        return type(self), (self._message,)


class Cache(object):

    def __init__(self, dsn: Text) -> None:
        self._dsn = dsn
        self._pool = None
        self._raii = None

    async def __aenter__(self) -> 'Cache':
        with cl.ExitStack() as stack:
            self._pool = stack.enter_context(cf.ProcessPoolExecutor())
            await self._bg(initialize)
            self._raii = stack.pop_all()
        return self

    async def __aexit__(self, type_, value, traceback) -> bool:
        self._raii.close()
        self._pool = None
        self._raii = None

    async def get_root_id(self) -> Text:
        return await self.get_metadata('root_id')

    async def get_root_node(self) -> 'Node':
        root_id = await self.get_root_id()
        return await self.get_node_by_id(root_id)

    async def get_metadata(self, key: Text) -> Text:
        return await self._bg(get_metadata, key)

    async def set_metadata(self, key: Text, value: Text) -> None:
        return await self._bg(set_metadata, key, value)

    async def get_node_by_id(self, node_id: Text) -> Union['Node', None]:
        return await self._bg(get_node_by_id, node_id)

    async def get_node_by_path(self, path: Text) -> Union['Node', None]:
        path = pl.Path(path)
        parts = list(path.parts)
        if parts[0] != '/':
            raise ValueError('invalid path')

        root_id = await self.get_root_id()
        parts.pop(0)
        return await self._bg(get_node_by_path, root_id, parts)

    async def get_path_by_id(self, node_id: Text) -> Union[Text, None]:
        return await self._bg(get_path_by_id, node_id)

    async def get_node_by_name_from_parent_id(self,
        name: Text,
        parent_id: Text
    ) -> Union['Node', None]:
        return await self._bg(get_node_by_name_from_parent_id, name, parent_id)

    async def get_children_by_id(self, node_id: Text) -> List['Node']:
        return await self._bg(get_children_by_id, node_id)

    async def apply_changes(self,
        changes: Dict[Text, Any],
        check_point: Text,
    ) -> None:
        return await self._bg(apply_changes, changes, check_point)

    async def insert_node(self, node: 'Node') -> None:
        return await self._bg(insert_node, node)

    async def find_nodes_by_regex(self, pattern: Text) -> List['Node']:
        return await self._bg(find_nodes_by_regex, pattern)

    async def find_duplicate_nodes(self) -> List['Node']:
        return await self._bg(find_duplicate_nodes)

    async def find_orphan_nodes(self) -> List['Node']:
        return await self._bg(find_orphan_nodes)

    async def find_multiple_parents_nodes(self) -> List['Node']:
        return await self._bg(find_multiple_parents_nodes)

    async def _bg(self, fn, *args):
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(self._pool, fn, self._dsn, *args)


class Node(object):

    def __init__(self,
        id_: Text,
        name: Text,
        trashed: bool,
        created: arrow.Arrow,
        modified: arrow.Arrow,
        parent_list: List[Text],
        is_folder: bool,
        mime_type: Text,
        md5: Text,
        size: int,
        image: Dict[Text, Any],
        video: Dict[Text, Any],
    ) -> None:
        self._id = id_
        self._name = name
        self._trashed = trashed
        self._created = created
        self._modified = modified
        self._parent_list = parent_list
        self._is_folder = is_folder
        self._mime_type = mime_type
        self._md5 = md5
        self._size = size
        self._image = image
        self._video = video

    def __repr__(self):
        return f"Node(id='{self.id_}')"

    @property
    def is_root(self) -> bool:
        return self._name is None

    @property
    def id_(self) -> Text:
        return self._id

    @property
    def name(self) -> Text:
        return self._name

    @property
    def trashed(self) -> bool:
        return self._trashed

    @trashed.setter
    def trashed(self, trashed: bool) -> None:
        self._trashed = trashed

    @property
    def created(self) -> arrow.Arrow:
        return self._created

    @property
    def modified(self) -> arrow.Arrow:
        return self._modified

    @property
    def parent_list(self) -> List[Text]:
        return self._parent_list

    @property
    def parent_id(self) -> Text:
        return None if not self._parent_list else self._parent_list[0]

    @property
    def is_file(self) -> bool:
        return not self._is_folder

    @property
    def is_folder(self) -> bool:
        return self._is_folder

    @property
    def mime_type(self) -> Optional[Text]:
        return self._mime_type

    @property
    def md5(self) -> Optional[Text]:
        return self._md5

    @property
    def size(self) -> Optional[int]:
        return self._size

    @property
    def is_image(self) -> bool:
        return self._image is not None

    @property
    def image_width(self) -> Optional[int]:
        return self._image['width'] if self.is_image else None

    @property
    def image_height(self) -> Optional[int]:
        return self._image['height'] if self.is_image else None

    @property
    def is_video(self) -> bool:
        return self._video is not None

    @property
    def video_width(self) -> Optional[int]:
        return self._video['width'] if self.is_video else None

    @property
    def video_height(self) -> Optional[int]:
        return self._video['height'] if self.is_video else None

    @property
    def video_ms_duration(self) -> Optional[int]:
        return self._video['ms_duration'] if self.is_video else None


def dict_from_api(data: Dict[Text, Any]) -> Dict[Text, Any]:
    id_ = data['id']
    is_folder = data['mimeType'] == u.FOLDER_MIME_TYPE
    image = None
    if 'imageMediaMetadata' in data:
        image = {
            'width': data['imageMediaMetadata']['width'],
            'height': data['imageMediaMetadata']['height'],
        }
    video = None
    if 'videoMediaMetadata' in data:
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
        'md5': data.get('md5Checksum', None),
        'size': data.get('size', None),
        'image': image,
        'video': video,
    }


def node_from_api(data: Dict[Text, Any]) -> Node:
    data = dict_from_api(data)
    return node_from_database(data)


def node_from_database(data: Dict[Text, Any]) -> Node:
    return Node(
        id_=data['id'],
        name=data['name'],
        trashed=bool(data['trashed']),
        created=arrow.get(data['created']),
        modified=arrow.get(data['modified']),
        parent_list=data.get('parent_list', None),
        is_folder=data['is_folder'],
        mime_type=data['mime_type'],
        md5=data['md5'],
        size=data['size'],
        image=data['image'],
        video=data['video'],
    )


def dict_from_node(node):
    return {
        'id': node.id_,
        'parent_id': node.parent_id,
        'name': node.name,
        'trashed': node.trashed,
        'is_folder': node.is_folder,
        'created': node.created.isoformat(),
        'modified': node.modified.isoformat(),
        'mime_type': node.mime_type,
        'md5': node.md5,
        'size': node.size,
        'is_image': node.is_image,
        'image_width': node.image_width,
        'image_height': node.image_height,
        'is_video': node.is_video,
        'video_width': node.video_width,
        'video_height': node.video_height,
        'video_ms_duration': node.video_ms_duration,
    }


class Database(object):

    def __init__(self, dsn: Text) -> None:
        self._dsn = dsn

    def __enter__(self) -> sqlite3.Connection:
        self._db = sqlite3.connect(self._dsn)
        self._db.row_factory = sqlite3.Row
        return self._db

    def __exit__(self, type_, value, traceback) -> bool:
        self._db.close()


class ReadOnly(object):

    def __init__(self, db: sqlite3.Connection) -> None:
        self._db = db

    def __enter__(self) -> sqlite3.Cursor:
        self._cursor = self._db.cursor()
        return self._cursor

    def __exit__(self, type_, value, traceback) -> bool:
        self._cursor.close()


class ReadWrite(object):

    def __init__(self, db: sqlite3.Connection) -> None:
        self._db = db

    def __enter__(self) -> sqlite3.Cursor:
        self._cursor = self._db.cursor()
        return self._cursor

    def __exit__(self, type_, value, traceback) -> bool:
        if type_ is None:
            self._db.commit()
        else:
            self._db.rollback()
        self._cursor.close()


def initialize(dsn: Text):
    with Database(dsn) as db:
        try:
            # initialize table
            with ReadWrite(db) as query:
                for sql in SQL_CREATE_TABLES:
                    query.execute(sql)
        except sqlite3.OperationalError as e:
            pass

        # check the schema version
        with ReadOnly(db) as query:
            query.execute('PRAGMA user_version;')
            rv = query.fetchone()
        version = int(rv[0])

        if version < 3:
            raise CacheError((
                'impossible to migrate from old schema prior to'
                ' version 3, please rebuild the cache'
            ))


def get_metadata(dsn: Text, key: Text) -> Text:
    with Database(dsn) as db, \
         ReadOnly(db) as query:
        query.execute('SELECT value FROM metadata WHERE key = ?;', (key,))
        rv = query.fetchone()
    if not rv:
        raise KeyError
    return rv['value']


def set_metadata(dsn: Text, key: Text, value: Text) -> None:
    with Database(dsn) as db, \
         ReadWrite(db) as query:
        inner_set_metadata(query, key, value)


def get_node_by_id(dsn: Text, node_id: Text) -> Union['Node', None]:
    with Database(dsn) as db, \
         ReadOnly(db) as query:
        return inner_get_node_by_id(query, node_id)


def get_node_by_path(
    dsn: Text,
    root_id: Text,
    path_parts: List[Text],
) -> Union['Node', None]:
    node_id = root_id
    parts = path_parts
    with Database(dsn) as db, \
         ReadOnly(db) as query:
        for part in parts:
            query.execute('''
                SELECT nodes.id AS id
                FROM parentage
                    INNER JOIN nodes ON parentage.child=nodes.id
                WHERE parentage.parent=? AND nodes.name=?
            ;''', (node_id, part))
            rv = query.fetchone()
            if not rv:
                return None
            node_id = rv['id']

        node = inner_get_node_by_id(query, node_id)
    return node


def get_path_by_id(dsn: Text, node_id: Text) -> Union[Text, None]:
    parts = []
    with Database(dsn) as db, \
         ReadOnly(db) as query:
        while True:
            query.execute('''
                SELECT name
                FROM nodes
                WHERE id=?
            ;''', (node_id,))
            rv = query.fetchone()
            if not rv:
                raise CacheError(f'cannot find name for {node_id}')

            name = rv['name']
            if not name:
                parts.insert(0, '/')
                break
            parts.insert(0, name)

            query.execute('''
                SELECT parent
                FROM parentage
                WHERE child=?
            ;''', (node_id,))
            rv = query.fetchone()
            if not rv:
                # orphan node
                break
            node_id = rv['parent']

    path = pl.Path(*parts)
    return str(path)


def get_node_by_name_from_parent_id(
    dsn: Text,
    name: Text,
    parent_id: Text
) -> Union['Node', None]:
    with Database(dsn) as db, \
         ReadOnly(db) as query:
        query.execute('''
            SELECT nodes.id AS id
            FROM nodes
                INNER JOIN parentage ON parentage.child=nodes.id
            WHERE parentage.parent=? AND nodes.name=?
        ;''', (parent_id, name))
        rv = query.fetchone()

        if not rv:
            return None

        node = inner_get_node_by_id(query, rv['id'])
    return node


def get_children_by_id(dsn: Text, node_id: Text) -> List['Node']:
    with Database(dsn) as db, \
         ReadOnly(db) as query:
        query.execute('''
            SELECT child
            FROM parentage
            WHERE parent=?
        ;''', (node_id,))
        rv = query.fetchall()

        children = [inner_get_node_by_id(query, _['child']) for _ in rv]
    return children


def apply_changes(
    dsn: Text,
    changes: Dict[Text, Any],
    check_point: Text,
) -> None:
    with Database(dsn) as db, \
         ReadWrite(db) as query:
        for change in changes:
            is_removed = change['removed']
            if is_removed:
                inner_delete_node_by_id(query, change['fileId'])
                continue

            file_ = change['file']
            is_shared = file_['shared']
            is_owned_by_me = file_['ownedByMe']
            if is_shared or not is_owned_by_me:
                continue

            node = node_from_api(file_)
            inner_insert_node(query, node)

        inner_set_metadata(query, 'check_point', check_point)


def insert_node(dsn: Text, node: 'Node') -> None:
    with Database(dsn) as db, \
         ReadWrite(db) as query:
        inner_insert_node(query, node)

        if not node.name:
            inner_set_metadata(query, 'root_id', node.id_)


def find_nodes_by_regex(dsn: Text, pattern: Text) -> List['Node']:
    with Database(dsn) as db, \
         ReadOnly(db) as query:
        fn = ft.partial(sqlite3_regexp, re.compile(pattern, re.I))
        db.create_function('REGEXP', 2, fn)
        query.execute('SELECT id FROM nodes WHERE name REGEXP ?;', (pattern,))
        rv = query.fetchall()
        rv = [inner_get_node_by_id(query, _['id']) for _ in rv]
    return rv


def find_duplicate_nodes(dsn: Text) -> List['Node']:
    with Database(dsn) as db, \
         ReadOnly(db) as query:
        query.execute('''
            SELECT nodes.name AS name, parentage.parent as parent_id
            FROM nodes
                INNER JOIN parentage ON parentage.child=nodes.id
            GROUP BY parentage.parent, nodes.name
            HAVING COUNT(*) > 1
        ;''')
        name_parent_pair = query.fetchall()

        node_id_list = []
        for pair in name_parent_pair:
            query.execute('''
                SELECT nodes.id AS id
                FROM nodes
                    INNER JOIN parentage ON parentage.child=nodes.id
                WHERE parentage.parent=? AND nodes.name=?
            ;''', (pair['parent_id'], pair['name']))
            rv = query.fetchall()
            rv = (_['id'] for _ in rv)
            node_id_list.extend(rv)

        rv = [inner_get_node_by_id(query, _) for _ in node_id_list]
    return rv


def find_orphan_nodes(dsn: Text) -> List['Node']:
    with Database(dsn) as db, \
         ReadOnly(db) as query:
        query.execute('''
            SELECT nodes.id AS id
            FROM parentage
                LEFT OUTER JOIN nodes ON parentage.child=nodes.id
            WHERE parentage.parent IS NULL
        ;''')
        rv = query.fetchall()
        rv = [inner_get_node_by_id(query, _['id']) for _ in rv]
    return rv


def find_multiple_parents_nodes(dsn: Text) -> List['Node']:
    with Database(dsn) as db, \
         ReadOnly(db) as query:
        query.execute('''
            SELECT child, COUNT(child) AS parent_count
            FROM parentage
            GROUP BY child
            HAVING parent_count > 1
        ;''')
        rv = query.fetchall()
        rv = [inner_get_node_by_id(query, _['child']) for _ in rv]
    return rv


def inner_set_metadata(query: Text, key: Text, value: Text) -> None:
    query.execute('''
        INSERT OR REPLACE INTO metadata
        VALUES (?, ?)
    ;''', (key, value))


def inner_get_node_by_id(
    query: sqlite3.Cursor,
    node_id: Text,
) -> Union['Node', None]:
    query.execute('''
        SELECT name, trashed, created, modified
        FROM nodes
        WHERE id=?
    ;''', (node_id,))
    rv = query.fetchone()
    if not rv:
        return None
    node = dict(rv)
    node['id'] = node_id

    query.execute('''
        SELECT mime_type, md5, size
        FROM files
        WHERE id=?
    ;''', (node_id,))
    rv = query.fetchone()
    is_folder = rv is None
    node['is_folder'] = is_folder
    node['mime_type'] = None if is_folder else rv['mime_type']
    node['md5'] = None if is_folder else rv['md5']
    node['size'] = None if is_folder else rv['size']

    query.execute('''
        SELECT parent
        FROM parentage
        WHERE child=?
    ;''', (node_id,))
    rv = query.fetchall()
    node['parent_list'] = None if not rv else [_['parent'] for _ in rv]

    query.execute('''
        SELECT width, height
        FROM images
        WHERE id=?
    ;''', (node_id,))
    rv = query.fetchone()
    node['image'] = {
        'width': rv['width'],
        'height': rv['height'],
    } if rv else None

    query.execute('''
        SELECT width, height, ms_duration
        FROM videos
        WHERE id=?
    ;''', (node_id,))
    rv = query.fetchone()
    node['video'] = {
        'width': rv['width'],
        'height': rv['height'],
        'ms_duration': rv['ms_duration'],
    } if rv else None

    node = node_from_database(node)
    return node


def inner_insert_node(query: sqlite3.Cursor, node: Node) -> None:
    # add this node
    query.execute('''
        INSERT OR REPLACE INTO nodes
        (id, name, trashed, created, modified)
        VALUES
        (?, ?, ?, ?, ?)
    ;''', (node.id_, node.name, node.trashed,
           node.created.timestamp, node.modified.timestamp))

    # add file information
    if not node.is_folder:
        query.execute('''
            INSERT OR REPLACE INTO files
            (id, mime_type, md5, size)
            VALUES
            (?, ?, ?, ?)
        ;''', (node.id_, node.mime_type, node.md5, node.size))

    # remove old parentage
    query.execute('''
        DELETE FROM parentage
        WHERE child=?
    ;''', (node.id_,))
    # add parentage if there is any
    if node.parent_list:
        for parent in node.parent_list:
            query.execute('''
                INSERT INTO parentage
                (parent, child)
                VALUES
                (?, ?)
            ;''', (parent, node.id_))

    # add image information
    if node.is_image:
        query.execute('''
            INSERT OR REPLACE INTO images
            (id, width, height)
            VALUES
            (?, ?, ?)
        ;''', (node.id_, node.image_width, node.image_height))

    # add video information
    if node.is_video:
        query.execute('''
            INSERT OR REPLACE INTO videos
            (id, width, height, ms_duration)
            VALUES
            (?, ?, ?, ?)
        ;''', (node.id_, node.video_width, node.video_height,
               node.video_ms_duration))


def inner_delete_node_by_id(query: sqlite3.Cursor, node_id: Text) -> None:
    # remove from videos
    query.execute('''
        DELETE FROM videos
        WHERE id=?
    ;''', (node_id,))

    # remove from images
    query.execute('''
        DELETE FROM images
        WHERE id=?
    ;''', (node_id,))

    # disconnect parents
    query.execute('''
        DELETE FROM parentage
        WHERE child=? OR parent=?
    ;''', (node_id, node_id))

    # remove from files
    query.execute('''
        DELETE FROM files
        WHERE id=?
    ;''', (node_id,))

    # remove from nodes
    query.execute('''
        DELETE FROM nodes
        WHERE id=?
    ;''', (node_id,))


def sqlite3_regexp(
    pattern: 'Pattern',
    _: Text,
    cell: Union[Text, None],
) -> bool:
    if cell is None:
        # root node
        return False
    return pattern.search(cell) is not None
