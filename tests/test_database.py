import contextlib as cl
import os
import sqlite3
import tempfile
import unittest as ut

import wcpan.drive.google.database as wdgdb


class TestTransaction(ut.TestCase):

    def setUp(self):
        _, self._file = tempfile.mkstemp()
        with connect(self._file) as db:
            prepare(db)

    def tearDown(self):
        os.unlink(self._file)

    def testRead(self):
        with connect(self._file) as db:
            with wdgdb.ReadOnly(db) as query:
                inner_select(query)
                rv = query.fetchone()

        self.assertIsNotNone(rv)
        self.assertEqual(rv['id'], 1)

    def testWrite(self):
        with connect(self._file) as db:
            with wdgdb.ReadWrite(db) as query:
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
            with wdgdb.ReadOnly(db1) as q1:
                inner_select(q1)
                with wdgdb.ReadOnly(db2) as q2:
                    inner_select(q2)

    def testWriteWhileReading(self):
        with connect(self._file) as rdb, \
             connect(self._file) as wdb:
            with self.assertRaises(sqlite3.OperationalError) as e:
                with wdgdb.ReadOnly(rdb) as rq:
                    inner_select(rq)
                    with wdgdb.ReadWrite(wdb) as wq:
                        inner_insert(wq)

        self.assertEqual(str(e.exception), 'database is locked')

    def testReadWhileWriting(self):
        with connect(self._file) as rdb, \
             connect(self._file) as wdb:
            with wdgdb.ReadWrite(wdb) as wq:
                inner_insert(wq)
                with wdgdb.ReadOnly(rdb) as rq:
                    rq.execute('''
                        SELECT id FROM student WHERE name=?;
                    ''', ('bob',))
                    rv = rq.fetchone()

        self.assertIsNone(rv)

    def testParallelWriting(self):
        with connect(self._file) as db1, \
             connect(self._file) as db2:
            with self.assertRaises(sqlite3.OperationalError) as e:
                with wdgdb.ReadWrite(db1) as q1:
                    inner_insert(q1)
                    with wdgdb.ReadWrite(db2) as q2:
                        inner_insert(q2)

        self.assertEqual(str(e.exception), 'database is locked')


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
