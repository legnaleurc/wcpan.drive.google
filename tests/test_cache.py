import asyncio
import contextlib as cl
import datetime as dt
import functools as ft
import os
import sqlite3
import tempfile
import unittest as ut
import unittest.mock as utm

import wcpan.drive.google.cache as wdgc
from wcpan.drive.google.util import FOLDER_MIME_TYPE
import wcpan.worker as ww


class TestTransaction(ut.TestCase):

    def setUp(self):
        _, self._file = tempfile.mkstemp()
        with connect(self._file) as db:
            prepare(db)

    def tearDown(self):
        os.unlink(self._file)

    def testRead(self):
        with connect(self._file) as db:
            with wdgc.ReadOnly(db) as query:
                inner_select(query)
                rv = query.fetchone()

        self.assertIsNotNone(rv)
        self.assertEqual(rv['id'], 1)

    def testWrite(self):
        with connect(self._file) as db:
            with wdgc.ReadWrite(db) as query:
                inner_insert(query)

            with cl.closing(db.cursor()) as query:
                query.execute('''
                    SELECT id FROM student WHERE name=?;
                ''', ('bob',))
                rv = query.fetchone()

        self.assertIsNotNone(rv)
        self.assertEqual(rv['id'], 2)

    def testParallelReading(self):
        with connect(self._file) as db1, \
             connect(self._file) as db2:
            with wdgc.ReadOnly(db1) as q1:
                inner_select(q1)
                with wdgc.ReadOnly(db2) as q2:
                    inner_select(q2)

    def testWriteWhileReading(self):
        with connect(self._file) as rdb, \
             connect(self._file) as wdb:
            with self.assertRaises(sqlite3.OperationalError) as e:
                with wdgc.ReadOnly(rdb) as rq:
                    inner_select(rq)
                    with wdgc.ReadWrite(wdb) as wq:
                        inner_insert(wq)

        self.assertEqual(str(e.exception), 'database is locked')

    def testReadWhileWriting(self):
        with connect(self._file) as rdb, \
             connect(self._file) as wdb:
            with wdgc.ReadWrite(wdb) as wq:
                inner_insert(wq)
                with wdgc.ReadOnly(rdb) as rq:
                    rq.execute('''
                        SELECT id FROM student WHERE name=?;
                    ''', ('bob',))
                    rv = rq.fetchone()

        self.assertIsNone(rv)

    def testParallelWriting(self):
        with connect(self._file) as db1, \
             connect(self._file) as db2:
            with self.assertRaises(sqlite3.OperationalError) as e:
                with wdgc.ReadWrite(db1) as q1:
                    inner_insert(q1)
                    with wdgc.ReadWrite(db2) as q2:
                        inner_insert(q2)

        self.assertEqual(str(e.exception), 'database is locked')


class TestNodeCache(ut.TestCase):

    @ww.sync
    async def setUp(self):
        _, self._file = tempfile.mkstemp()

        async with cl.AsyncExitStack() as ctx:
            self._db = await ctx.enter_async_context(wdgc.Cache(self._file))
            self._stack = ctx.pop_all()

        await initial_nodes(self._db)

    @ww.sync
    async def tearDown(self):
        await self._stack.aclose()
        os.unlink(self._file)

    @ww.sync
    async def testRoot(self):
        node = await self._db.get_root_node()
        self.assertEqual(node.id_, '__ROOT_ID__')

    @ww.sync
    async def testSearch(self):
        nodes = await self._db.find_nodes_by_regex(r'^f1$')
        self.assertEqual(len(nodes), 1)
        node = nodes[0]
        self.assertEqual(node.id_, '__F1_ID__')
        path = await self._db.get_path_by_id(node.id_)
        self.assertEqual(path, '/d1/f1')


def connect(path):
    db = sqlite3.connect(path, timeout=0.1)
    db.row_factory = sqlite3.Row
    return db


def prepare(db):
    with cl.closing(db.cursor()) as query:
        query.execute('''
            CREATE TABLE student (
                id INTEGER NOT NULL,
                name VARCHAR(64),
                PRIMARY KEY (id)
            );
        ''')
        query.execute('''
            INSERT INTO student
            (id, name)
            VALUES
            (?, ?);
        ''', (1, 'alice'))


def inner_select(query):
    query.execute('''
        SELECT id FROM student WHERE name=?;
    ''', ('alice',))


def inner_insert(query):
    query.execute('''
        INSERT INTO student
        (id, name)
        VALUES
        (?, ?);
    ''', (2, 'bob'))


def get_utc_now():
    return dt.datetime.now(dt.timezone.utc)


async def initial_nodes(db):
    data = {
        'id': '__ROOT_ID__',
        'name': '',
        'mimeType': FOLDER_MIME_TYPE,
        'trashed': False,
        'createdTime': get_utc_now().isoformat(),
        'modifiedTime': get_utc_now().isoformat(),
    }
    node = wdgc.node_from_api(data)
    await db.insert_node(node)

    data = [
        {
            'removed': False,
            'file': {
                'id': '__D1_ID__',
                'name': 'd1',
                'mimeType': FOLDER_MIME_TYPE,
                'trashed': False,
                'createdTime': get_utc_now().isoformat(),
                'modifiedTime': get_utc_now().isoformat(),
                'parents': ['__ROOT_ID__'],
            },
        },
        {
            'removed': False,
            'file': {
                'id': '__D2_ID__',
                'name': 'd2',
                'mimeType': FOLDER_MIME_TYPE,
                'trashed': False,
                'createdTime': get_utc_now().isoformat(),
                'modifiedTime': get_utc_now().isoformat(),
                'parents': ['__ROOT_ID__'],
            }
        },
        {
            'removed': False,
            'file': {
                'id': '__F1_ID__',
                'name': 'f1',
                'mimeType': 'text/plain',
                'trashed': False,
                'createdTime': get_utc_now().isoformat(),
                'modifiedTime': get_utc_now().isoformat(),
                'parents': ['__D1_ID__'],
                'md5Checksum': '__F1_MD5__',
                'size': 1337,
            }
        },
        {
            'removed': False,
            'file': {
                'id': '__F2_ID__',
                'name': 'f2',
                'mimeType': 'text/plain',
                'trashed': False,
                'createdTime': get_utc_now().isoformat(),
                'modifiedTime': get_utc_now().isoformat(),
                'parents': ['__D2_ID__'],
                'md5Checksum': '__F2_MD5__',
                'size': 1234,
            }
        },
    ]
    await db.apply_changes(data, '2')
