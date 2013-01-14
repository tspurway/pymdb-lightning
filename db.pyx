cimport cmdb
from libc.stdlib cimport malloc, free
from libc.string cimport memcpy, strlen

# env creation flags
MDB_FIXEDMAP = 0x01
MDB_NOSUBDIR = 0x4000
MDB_NOSYNC = 0x10000
MDB_RDONLY = 0x20000
MDB_NOMETASYNC = 0x40000
MDB_WRITEMAP = 0x80000
MDB_MAPASYNC = 0x100000

# db open flags
MDB_REVERSEKEY = 0x02
MDB_DUPSORT = 0x04
MDB_INTEGERKEY = 0x08
MDB_DUPFIXED = 0x10
MDB_INTEGERDUP = 0x20
MDB_REVERSEDUP = 0x40
MDB_CREATE = 0x40000

# write flags
MDB_NOOVERWRITE = 0x10
MDB_NODUPDATA = 0x20
MDB_CURRENT = 0x40
MDB_RESERVE = 0x10000
MDB_APPEND = 0x20000
MDB_APPENDDUP = 0x40000
MDB_MULTIPLE = 0x80000

# cursor operations
MDB_FIRST = 0
MDB_FIRST_DUP = 1
MDB_GET_BOTH = 2
MDB_GET_BOTH_RANGE = 3
MDB_GET_CURRENT = 4
MDB_GET_MULTIPLE = 5
MDB_LAST = 6
MDB_LAST_DUP = 7
MDB_NEXT = 8
MDB_NEXT_DUP = 9
MDB_NEXT_MULTIPLE = 10
MDB_NEXT_NODUP = 11
MDB_PREV = 12
MDB_PREV_DUP = 13
MDB_PREV_NODUP = 14
MDB_SET = 15
MDB_SET_KEY = 16
MDB_SET_RANGE = 17

# constants for byte sizes
KB = 1024
MB = 1000 * KB
GB = 1000 * MB
TB = 1000 * GB


cdef class Txn:
    cdef cmdb.MDB_txn *txn

    def __init__(self, Env env, Txn parent=None, int flags=0):
        cdef cmdb.MDB_txn *parent_txn = NULL
        if parent:
            parent_txn = parent.txn

        err = cmdb.mdb_txn_begin(env.env, parent_txn, flags, &self.txn)
        if err:
            raise Exception("Error creating master transaction: %s" % cmdb.mdb_strerror(err))

    def commit(self):
        err = cmdb.mdb_txn_commit(self.txn)
        if err:
            raise Exception("Error commiting transaction: %s" % cmdb.mdb_strerror(err))

    def abort(self):
        cmdb.mdb_txn_abort(self.txn)


cdef class Env:
    cdef cmdb.MDB_env *env

    def __init__(self, char *filename, int flags=MDB_FIXEDMAP, int permissions=0664,
                 int mapsize=0, int max_dbs=0, int max_readers=0):
        err = cmdb.mdb_env_create(&self.env)
        if err:
            raise Exception("Error creating environment: %s" % cmdb.mdb_strerror(err))
        if mapsize:
            err = cmdb.mdb_env_set_mapsize(self.env, mapsize)
            if err:
                raise Exception("Could not set environment size: %s" % cmdb.mdb_strerror(err))
        if max_readers:
            err = cmdb.mdb_env_set_maxreaders(self.env, max_readers)
            if err:
                raise Exception("Could not set max readers: %s" % cmdb.mdb_strerror(err))
        if max_dbs:
            err = cmdb.mdb_env_set_maxdbs(self.env, max_dbs)
            if err:
                raise Exception("Could not set max dbs: %s" % cmdb.mdb_strerror(err))

        err = cmdb.mdb_env_open(self.env, filename, flags, permissions)
        if err:
            raise Exception("Error opening environment: %s" % cmdb.mdb_strerror(err))

    def close(self):
        cmdb.mdb_env_close(self.env)

    def copy(self, char *filename):
        err = cmdb.mdb_env_copy(self.env, filename)
        if err:
            raise Exception("Error copying environment: %s" % cmdb.mdb_strerror(err))

    def stat(self):
        cdef cmdb.MDB_stat stat
        err = cmdb.mdb_env_stat(self.env, &stat)
        if err:
            raise Exception("Error 'stat'ing environment: %s" % cmdb.mdb_strerror(err))
        return dict(ms_psize=stat.ms_psize,
            ms_depth=stat.ms_depth,
            ms_branch_pages=stat.ms_branch_pages,
            ms_leaf_pages=stat.ms_leaf_pages,
            ms_overflow_pages=stat.ms_overflow_pages,
            ms_entries=stat.ms_entries)

    def info(self):
        cdef cmdb.MDB_envinfo info
        err = cmdb.mdb_env_info(self.env, &info)
        if err:
            raise Exception("Error 'info'ing environment: %s" % cmdb.mdb_strerror(err))
        return dict(
            me_mapaddr=<int>info.me_mapaddr,
            me_mapsize=info.me_mapsize,
            me_last_pgno=info.me_last_pgno,
            me_last_txnid=info.me_last_txnid,
            me_maxreaders=info.me_maxreaders,
            me_numreaders=info.me_numreaders,
        )

    def sync(self, force=False):
        cdef int cforce = 0
        if force: cforce = 1
        err = cmdb.mdb_env_sync(self.env, force)
        if err:
            raise Exception("Error sycning environment: %s" % cmdb.mdb_strerror(err))

    def set_flags(self, int flags, int onoff):
        err = cmdb.mdb_env_set_flags(self.env, flags, onoff)
        if err:
            raise Exception("Error setting flags in environment: %s" % cmdb.mdb_strerror(err))

    def get_flags(self):
        cdef unsigned int flags
        err = cmdb.mdb_env_get_flags(self.env, &flags)
        if err:
            raise Exception("Error getting flags in environment: %s" % cmdb.mdb_strerror(err))
        return flags

    def begin_txn(self, parent=None, flags=0):
        cdef txn = Txn(self, parent, flags)
        return txn

    def open_db(self, txn, name=None, flags=MDB_CREATE):
        cdef db = DB(self, txn, name, flags)
        return db


cdef class DB:
    cdef cmdb.MDB_dbi dbi
    cdef Env env

    def __init__(self, Env env, Txn txn, name, flags=MDB_FIXEDMAP|MDB_CREATE):
        self.env = env
        cdef char *cname = NULL
        if name:
            cname = <char*> name
        err = cmdb.mdb_dbi_open(txn.txn, cname, flags, &self.dbi)
        if err:
            raise Exception("Error opening datsabase: %s" % cmdb.mdb_strerror(err))

    def close(self):
        cmdb.mdb_dbi_close(self.env.env, self.dbi)

    def drop(self, Txn txn, bint delete):
        err = cmdb.mdb_drop(txn.txn, self.dbi, delete)
        if err:
            raise Exception("Error dropping datsabase: %s" % cmdb.mdb_strerror(err))

    def get(self, Txn txn, char *key):
        cdef cmdb.MDB_val api_key
        cdef cmdb.MDB_val api_value
        api_key.mv_size = strlen(key)
        api_key.mv_data = key

        err = cmdb.mdb_get(txn.txn, self.dbi, &api_key, &api_value)
        if err:
            raise Exception("Error getting data: %s" % cmdb.mdb_strerror(err))
        cdef char *rval = <char*>api_value.mv_data
        return (<char*>rval)[:api_value.mv_size]

    def put(self, Txn txn, char *key, char *value, int flags=0):
        cdef cmdb.MDB_val api_key
        cdef cmdb.MDB_val api_value
        cdef long key_len = strlen(key)
        cdef long value_len = strlen(value)

        api_key.mv_size = key_len
        api_key.mv_data = <void*>key
        api_value.mv_size = value_len
        api_value.mv_data = <void*>value

        err = cmdb.mdb_put(txn.txn, self.dbi, &api_key, &api_value, flags)
        if err:
            raise Exception("Error putting data: %s" % cmdb.mdb_strerror(err))

    def delete(self, Txn txn, char *key, char *value=NULL):
        cdef cmdb.MDB_val api_key
        cdef cmdb.MDB_val api_value
        cdef long key_len = strlen(key)
        cdef long value_len = 0
        if value:
            value_len = strlen(value)

        api_key.mv_size = key_len
        api_key.mv_data = <void*>key
        api_value.mv_size = value_len
        api_value.mv_data = <void*>value
        err = cmdb.mdb_del(txn.txn, self.dbi, &api_key, &api_value)

    def items(self, Txn txn):
        cursor = Cursor(txn, self)
        while True:
            key, value = cursor.get()
            if key:
                yield key, value
            else:
                break
        cursor.close()

cdef class Cursor:
    cdef cmdb.MDB_cursor *cursor

    def __init__(self, Txn txn, DB dbi):
        err = cmdb.mdb_cursor_open(txn.txn, dbi.dbi, &self.cursor)
        if err:
            raise Exception("Error creating Cursor: %s" % cmdb.mdb_strerror(err))

    def close(self):
        cmdb.mdb_cursor_close(self.cursor)

    def renew(self, Txn txn):
        err = cmdb.mdb_cursor_renew(txn.txn, self.cursor)
        if err:
            raise Exception("Error renewing Cursor: %s" % cmdb.mdb_strerror(err))

    def get(self, char *key=NULL, char *value=NULL, int op=MDB_NEXT):
        cdef cmdb.MDB_val api_key
        cdef cmdb.MDB_val api_value
        cdef long key_len = 0
        if key:
            key_len = strlen(key)
        cdef long value_len = 0
        if value:
            value_len = strlen(value)

        api_key.mv_size = key_len
        api_key.mv_data = <void*>key
        api_value.mv_size = value_len
        api_value.mv_data = <void*>value

        if not cmdb.mdb_cursor_get(self.cursor, &api_key, &api_value, op):
            key = <char*>api_key.mv_data
            value = <char*>api_value.mv_data
            return (<char*>key)[:api_key.mv_size], (<char*>value)[:api_value.mv_size]
        else:
            return None, None

    def put(self, char *key, char *value, int flags=0):
        cdef cmdb.MDB_val api_key
        cdef cmdb.MDB_val api_value
        cdef long key_len = strlen(key)
        cdef long value_len = strlen(value)

        api_key.mv_size = key_len
        api_key.mv_data = <void*>key
        api_value.mv_size = value_len
        api_value.mv_data = <void*>value

        err = cmdb.mdb_cursor_put(self.cursor, &api_key, &api_value, flags)
        if err:
            raise Exception("Error putting Cursor: %s" % cmdb.mdb_strerror(err))


    def delete(self, int flags=0):
        err = cmdb.mdb_cursor_del(self.cursor, flags)
        if err:
            raise Exception("Error deleting Cursor: %s" % cmdb.mdb_strerror(err))

    def count_dups(self):
        cdef size_t rval = 0
        err = cmdb.mdb_cursor_count(self.cursor, &rval)
        if err:
            raise Exception("Error counting Cursor: %s" % cmdb.mdb_strerror(err))
        return rval
