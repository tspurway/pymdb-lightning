cdef extern from './lib/lmdb.h':
    ctypedef struct MDB_txn:
        pass

    ctypedef struct MDB_env:
        pass

    ctypedef struct MDB_cursor:
        pass

    ctypedef unsigned int    MDB_dbi

    ctypedef struct MDB_val:
        size_t   mv_size
        void    *mv_data

    ctypedef   struct MDB_stat:
        unsigned int    ms_psize
        unsigned int    ms_depth
        size_t          ms_branch_pages
        size_t          ms_leaf_pages
        size_t          ms_overflow_pages
        size_t          ms_entries

    ctypedef struct MDB_envinfo:
        void            *me_mapaddr
        size_t          me_mapsize
        size_t          me_last_pgno
        size_t          me_last_txnid
        unsigned int    me_maxreaders
        unsigned int    me_numreaders



    char *mdb_strerror(int err)
    int  mdb_env_create(MDB_env **env)
    int  mdb_env_open(MDB_env *env, char *path, unsigned int flags, unsigned int mode)
    int  mdb_env_copy(MDB_env *env, char *path)
    int  mdb_env_stat(MDB_env *env, MDB_stat *stat)
    int  mdb_env_info(MDB_env *env, MDB_envinfo *stat)
    int  mdb_env_sync(MDB_env *env, int force)
    int  mdb_env_set_flags(MDB_env *env, unsigned int flags, int onoff)
    int  mdb_env_get_flags(MDB_env *env, unsigned int *flags)
    int  mdb_env_get_path(MDB_env *env, char **path)
    int  mdb_env_set_mapsize(MDB_env *env, size_t size)
    int  mdb_env_set_maxdbs(MDB_env *env, MDB_dbi dbs)
    int  mdb_env_set_maxreaders(MDB_env *env, unsigned int readers)
    void mdb_env_close(MDB_env *env)

    int  mdb_txn_begin(MDB_env *env, MDB_txn *parent, unsigned int flags, MDB_txn **txn)
    int  mdb_txn_commit(MDB_txn *txn)
    void mdb_txn_abort(MDB_txn *txn)

    int  mdb_dbi_open(MDB_txn *txn, char *name, unsigned int flags, MDB_dbi *dbi)
    void mdb_dbi_close(MDB_env *env, MDB_dbi dbi)
    int  mdb_stat(MDB_txn *txn, MDB_dbi dbi, MDB_stat *stat)
    int  mdb_drop(MDB_txn *txn, MDB_dbi dbi, int delete)
    int  mdb_get(MDB_txn *txn, MDB_dbi dbi, MDB_val *key, MDB_val *data)
    int  mdb_put(MDB_txn *txn, MDB_dbi dbi, MDB_val *key, MDB_val *data, unsigned int flags)
    int  mdb_del(MDB_txn *txn, MDB_dbi dbi, MDB_val *key, MDB_val *data)

    int  mdb_cursor_open(MDB_txn *txn, MDB_dbi dbi, MDB_cursor **cursor)
    void mdb_cursor_close(MDB_cursor *cursor)
    int  mdb_cursor_renew(MDB_txn *txn, MDB_cursor *cursor)
    int  mdb_cursor_get(MDB_cursor *cursor, MDB_val *key, MDB_val *data, int op)
    int  mdb_cursor_put(MDB_cursor *cursor, MDB_val *key, MDB_val *data, unsigned int flags)
    int  mdb_cursor_del(MDB_cursor *cursor, unsigned int flags)
    int  mdb_cursor_count(MDB_cursor *cursor, size_t *countp)

