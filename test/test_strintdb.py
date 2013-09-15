# -*- coding: utf-8 -*-
import mdb
from unittest import TestCase


class TestDB(TestCase):

    def setUp(self):
        import os
        import errno
        self.path = './testdbm'
        try:
            os.makedirs(self.path)
        except OSError as e:
            if e.errno == errno.EEXIST and os.path.isdir(self.path):
                pass
            else:
                raise
        self.env = mdb.Env(self.path, mapsize=1 * mdb.MB, max_dbs=8)

    def tearDown(self):
        import shutil
        self.env.close()
        shutil.rmtree(self.path)

    def drop_mdb(self):
        txn = self.env.begin_txn()
        db = self.env.open_db(txn, 'test_db')
        db.drop(txn, 0)
        txn.commit()
        db.close()

    def test_drop(self):
        self.drop_mdb()
        txn = self.env.begin_txn()
        db = self.env.open_db(txn, 'test_db')
        items = db.items(txn)
        self.assertRaises(StopIteration, items.next)
        db.close()

    def test_put(self):
        # all keys must be sorted
        txn = self.env.begin_txn()
        db = self.env.open_db(txn, 'test_db', mdb.MDB_DUPSORT|mdb.MDB_INTEGERDUP|mdb.MDB_CREATE)
        db.put(txn, 'foo', 1)
        txn.commit()
        txn = self.env.begin_txn()
        self.assertEqual(db.get(txn, 'foo'), 1)
        db.close()

    def test_put_unicode(self):
        # all keys must be sorted
        txn = self.env.begin_txn()
        db = self.env.open_db(txn, 'test_db', mdb.MDB_DUPSORT|mdb.MDB_INTEGERDUP|mdb.MDB_CREATE)
        db.put(txn, 'fΩo', 2)
        txn.commit()
        txn = self.env.begin_txn()
        self.assertEqual(db.get(txn, 'fΩo'), 2)
        db.close()

    def test_get_exception(self):
        txn = self.env.begin_txn()
        db = self.env.open_db(txn, 'test_db', mdb.MDB_DUPSORT|mdb.MDB_INTEGERDUP|mdb.MDB_CREATE)
        with self.assertRaises(mdb.KeyNotFoundError):
            db.get(txn, "Not Existed")
        txn.commit()
        db.close()

    def test_put_duplicate(self):
        # all values must be sorted as well
        txn = self.env.begin_txn()
        db = self.env.open_db(txn, 'test_db', mdb.MDB_DUPSORT|mdb.MDB_INTEGERDUP|mdb.MDB_CREATE)
        db.put(txn, 'foo', 1)
        db.put(txn, 'foo', 2)
        txn.commit()
        txn = self.env.begin_txn()
        self.assertEqual([value for value in db.get_dup(txn, 'foo')],
                         [1, 2])
        db.close()

    def test_get_all_items(self):
        txn = self.env.begin_txn()
        db = self.env.open_db(txn, 'test_db', mdb.MDB_DUPSORT|mdb.MDB_INTEGERDUP|mdb.MDB_CREATE)
        db.put(txn, 'all', 1)
        db.put(txn, 'all1', 2)
        db.put(txn, 'all', 11)
        txn.commit()
        txn = self.env.begin_txn()
        self.assertEqual(list(db.dup_items(txn)),
                         [('all', 1), ('all', 11), ('all1', 2)])
        db.close()

    def test_delete_by_key(self):
        txn = self.env.begin_txn()
        db = self.env.open_db(txn, 'test_db', mdb.MDB_DUPSORT|mdb.MDB_INTEGERDUP|mdb.MDB_CREATE)
        db.put(txn, 'delete', 1)
        db.put(txn, 'delete', 11)
        txn.commit()
        txn = self.env.begin_txn()
        db.delete(txn, 'delete')
        txn.commit()
        txn = self.env.begin_txn()
        self.assertRaises(Exception, db.get, txn, 'delete')
        txn.abort()
        db.close()

    def test_delete_by_key_value(self):
        txn = self.env.begin_txn()
        db = self.env.open_db(txn, 'test_db', mdb.MDB_DUPSORT|mdb.MDB_INTEGERDUP|mdb.MDB_CREATE)
        db.put(txn, 'delete', 1)
        db.put(txn, 'delete', 11)
        txn.commit()
        txn = self.env.begin_txn()
        db.delete(txn, 'delete', 1)
        txn.commit()
        txn = self.env.begin_txn()
        self.assertEqual(db.get(txn, 'delete'), 11)
        db.close()
