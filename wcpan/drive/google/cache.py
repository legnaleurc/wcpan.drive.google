import asyncio
import contextlib as cl
import concurrent.futures as cf
import functools as ft
import pathlib as pl
import re
import sqlite3
from typing import Any, Dict, List, Text, Union

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
        PRIMARY KEY (id),
        UNIQUE (id)
    );
    ''',
    '''
    CREATE TABLE files (
        id TEXT NOT NULL,
        md5 TEXT,
        size INTEGER,
        PRIMARY KEY (id),
        UNIQUE (id),
        FOREIGN KEY (id) REFERENCES nodes (id)
    );
    ''',
    '''
    CREATE TABLE parentage (
        parent TEXT NOT NULL,
        child TEXT NOT NULL,
        PRIMARY KEY (parent, child),
        FOREIGN KEY (child) REFERENCES nodes (id)
    );
    ''',
    'CREATE INDEX ix_parentage_parent ON parentage(parent);',
    'CREATE INDEX ix_parentage_child ON parentage(child);',
    'CREATE INDEX ix_nodes_names ON nodes(name);',
    'CREATE INDEX ix_nodes_trashed ON nodes(trashed);',
    'CREATE INDEX ix_nodes_created ON nodes(created);',
    'CREATE INDEX ix_nodes_modified ON nodes(modified);',
    'PRAGMA user_version = 2;',
]


CURRENT_SCHEMA_VERSION = 2


class CacheError(u.GoogleDriveError):

    def __init__(self, message: Text) -> None:
        super(DatabaseError, self).__init__()

        self._message = message

    def __str__(self) -> Text:
        return self._message


class Cache(object):

    def __init__(self, dsn: Text) -> None:
        self._dsn = dsn
        self._loop = asyncio.get_event_loop()
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
            raise Exception('invalid path')

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

    async def _bg(self, fn, *args):
        return await self._loop.run_in_executor(self._pool, fn, self._dsn,
                                                *args)


class Node(object):

    @staticmethod
    def from_api(data: Dict[Text, Any]) -> 'Node':
        node = Node(data)
        node._initialize_from_api()
        return node

    @staticmethod
    def from_database(data: Dict[Text, Any]) -> 'Node':
        node = Node(data)
        node._initialize_from_database()
        return node

    def __init__(self, data: Dict[Text, Any]) -> None:
        self._data = data

    @property
    def is_root(self) -> bool:
        return self._name is None

    @property
    def is_file(self) -> bool:
        return not self._is_folder

    @property
    def is_folder(self) -> bool:
        return self._is_folder

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
    def parents(self) -> List[Text]:
        return self._parents

    @property
    def parent_id(self) -> Text:
        if len(self._parents) != 1:
            msg = 'expected only one parent, got: {0}'.format(self._parents)
            raise DatabaseError(msg)
        return self._parents[0]

    @property
    def md5(self) -> Text:
        return self._md5

    @property
    def size(self) -> int:
        return self._size

    def _initialize_from_api(self) -> None:
        data = self._data
        self._id = data['id']
        self._name = data['name']
        self._trashed = data['trashed']
        self._created = arrow.get(data['createdTime'])
        self._modified = arrow.get(data['modifiedTime'])
        self._parents = data.get('parents', None)

        self._is_folder = data['mimeType'] == u.FOLDER_MIME_TYPE
        self._md5 = data.get('md5Checksum', None)
        self._size = data.get('size', None)

    def _initialize_from_database(self) -> None:
        data = self._data
        self._id = data['id']
        self._name = data['name']
        self._trashed = bool(data['trashed'])
        self._created = arrow.get(data['created'])
        self._modified = arrow.get(data['modified'])
        self._parents = data.get('parents', None)

        self._is_folder = data['is_folder']
        self._md5 = data['md5']
        self._size = data['size']


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
        version = rv[0]

        if CURRENT_SCHEMA_VERSION > version:
            migrate(db, version)


def migrate(db: sqlite3.Connection, version: int) -> None:
    # version 1 -> 2
    db.create_function('IS_TRASHED', 1, lambda _: _ == 'TRASH')
    db.create_function('ISO_TO_INT', 1, lambda _: arrow.get(_).timestamp)

    SQL = [
        'ALTER TABLE nodes RENAME TO old_nodes;',
        '''
        CREATE TABLE nodes (
            id TEXT NOT NULL,
            name TEXT,
            trashed BOOLEAN,
            created INTEGER,
            modified INTEGER,
            PRIMARY KEY (id),
            UNIQUE (id)
        );
        ''',
        '''
        INSERT INTO nodes
            (id, name, trashed, created, modified)
        SELECT
            id, name, IS_TRASHED(status), ISO_TO_INT(created),
            ISO_TO_INT(modified)
        FROM old_nodes
        ;''',
        'DROP TABLE old_nodes;',
        'CREATE INDEX ix_nodes_trashed ON nodes(trashed);',
        'CREATE INDEX ix_nodes_created ON nodes(created);',
        'CREATE INDEX ix_nodes_modified ON nodes(modified);',
        'PRAGMA user_version = 2;',
    ]

    with ReadWrite(db) as query:
        for sql in SQL:
            query.execute(sql)


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
                return None

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
            else:
                node = Node.from_api(change['file'])
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
        SELECT id, name, trashed, created, modified
        FROM nodes
        WHERE id=?
    ;''', (node_id,))
    rv = query.fetchone()
    if not rv:
        return None
    node = dict(rv)

    query.execute('''
        SELECT id, md5, size
        FROM files
        WHERE id=?
    ;''', (node_id,))
    rv = query.fetchone()
    is_folder = rv is None
    node['is_folder'] = is_folder
    node['md5'] = None if is_folder else rv['md5']
    node['size'] = None if is_folder else rv['size']

    query.execute('''
        SELECT parent, child
        FROM parentage
        WHERE child=?
    ;''', (node_id,))
    rv = query.fetchall()
    node['parents'] = None if not rv else [_['parent'] for _ in rv]

    node = Node.from_database(node)
    return node


def inner_insert_node(query: sqlite3.Cursor, node: Node) -> None:
    # add this node
    query.execute('''
        INSERT OR REPLACE INTO nodes
        (id, name, trashed, created, modified)
        VALUES
        (?, ?, ?, ?, ?)
    ;''', (node.id_, node.name, node.trashed, node.created.timestamp,
           node.modified.timestamp))

    # add file information
    if not node.is_folder:
        query.execute('''
            INSERT OR REPLACE INTO files
            (id, md5, size)
            VALUES
            (?, ?, ?)
        ;''', (node.id_, node.md5, node.size))

    # add parentage
    if node.parents:
        query.execute('''
            DELETE FROM parentage
            WHERE child=?
        ;''', (node.id_,))

        for parent in node.parents:
            query.execute('''
                INSERT OR REPLACE INTO parentage
                (parent, child)
                VALUES
                (?, ?)
            ;''', (parent, node.id_))


def inner_delete_node_by_id(query: sqlite3.Cursor, node_id: Text) -> None:
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
