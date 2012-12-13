pymdb-lightning
===============

Python interface to OpenLDAP MDB (aka. lightning db) key/value store.

Official Symas MDB Site:  http://www.symas.com/mdb/

The MDB database is a memory mapped (mmap) based b+ tree key/value store.

It uses MVCC, which allows for lockless read/writes - which is very useful in a process heavy Python system.

It is fully ACID compliant, and allows for nested transactions.

It allows for mutliple database instances per mmap, which is a nice touch.

It's got an excellent and very rich API (duplicate key b+ trees, read/write/del cursors, burst writes, etc).

It is fast.  I compares very favourably to kyotocabinet (faster than KCH in my application).

It's tiny.  It compiles to a library < 32K in size.

Unfortunately, the C source for MDB is buried deep inside OpenLDAP.  I have copied the latest source into the lib/ directory, and will resync occasionally.

I have (barely) tested the software on OS/X - THIS IS PRE-ALPHA software!  Use at your own risk.

requires:
 - Python 2.7 (that is all I have tested with)
 - compatibly versioned Cython

Install
=======

    cd lib
    make
    (on OSX do:  make -f Makefile.osx)
    sudo make install
    cd ..
    python setup.py build_ext --inplace
    (bravely install to your system with: sudo python setup.py build_ext install)

Usage
=====

    import mdb

    env = mdb.Env('/tmp/crack', max_dbs=5)
    txn = env.begin_txn()
    db = env.open_db(txn)
    db.put(txn, 'hi', 'assinine')
    txn.commit()
    txn = env.begin_txn()
    print '"%s"' % db.get(txn, 'hi')  # --> assinine
    txn.close()
    db.close()
    env.close()



