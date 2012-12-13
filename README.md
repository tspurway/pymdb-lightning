pymdb-lightning
===============

Python interface to OpenLDAP MDB (aka. lightning db) key/value store.

Official Symas MDB Site:  http://www.symas.com/mdb/

The MDB database is a memory mapped (mmap) based b+ tree based key/value store.

It uses MVCC, which allows for lockless read/writes - which is very useful in a process heavy Python system.

It is fully ACID compliant, and allows for nested transactions.

It allows for mutliple database instances per mmap, which is a nice touch.

It's got an excellent and very rich API (duplicate key b+ trees, read/write/del cursors, burst writes, etc).

It is fast.  I compares very favourably to kyotocabinet (faster than KCH in my application).

Unfortunately, the C source for MDB is buried deep inside OpenLDAP.  I have copied the latest source into the lib/ directory, and will resync occasionally.

I have tested the software on OS/X - THIS IS PRE-ALPHA software!  Use at your own risk.

requires:
 - Python 2.7 (that is all I have tested with)
 - compatibly versioned Cython

Install
=======

cd lib
make
sudo make install
cd ..
python setup.py build_ext --inplace

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



