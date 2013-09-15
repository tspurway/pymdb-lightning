pymdb-lightning
===============

Python interface to OpenLDAP MDB (aka. lightning db) key/value store.

Official Symas MDB Site:  http://www.symas.com/mdb/
MDB git repo:

The MDB database is a memory mapped (mmap) based b+ tree key/value store.

It uses MVCC, which allows for lockless read/writes - which is very useful in a process heavy Python system.

It is fully ACID compliant, and allows for nested transactions.

It allows for mutliple database instances per mmap, which is a nice touch.

It's got an excellent and very rich API (duplicate key b+ trees, read/write/del cursors, burst writes, etc).

It is fast.  I compares very favourably to kyotocabinet (faster than KCH in my application).

It's tiny.  It compiles to a library < 32K in size.



Requires:
=======
 - mdb
 - Python 2.7 (that is all I have tested with)
 - compatibly versioned Cython

Install
=======

    (in your src directory)
    git clone https://github.com/tspurway/pymdb-lightning.git
    git clone https://git.gitorious.org/mdb/mdb.git
    
    cd mdb/libraries/liblmdb/
    make
    sudo make install
    
    cd ../../../pymdb-lightning
    python setup.py build_ext --inplace
    (bravely install to your system with: sudo python setup.py install)

Usage
=====

Using Writer and Reader
-----------------------

    >>> import mdb
    >>> writer = mdb.Writer('/tmp/mdbtest')
    >>> writer.put('foo', 'bar')
    >>> writer.mput({"key": "value", "egg": "spam"})
    >>> writer.close()
    >>> reader = mdb.Reader('/tmp/mdbtest')
    >>> reader.get('foo')
    >>> for key, value in reader.iteritems():
    ...   print key, value
    >>> reader.close()

Using Integer Key
-----------------
    >>> writer = mdb.Writer('/tmp/mdbtest', dup=True, int_key=True)
    >>> writer = writer.put(1, 'foo')
    >>> writer = writer.put(1, 'bar')  # append a duplicate key
    >>> writer.close()
    >>> reader = mdb.DupReader('/tmp/mdbtest', int_key=True)
    >>> for v in reader.get(1):
    ...   print v
    >>> reader.close()
    
Using Low-level MDB
-------------------
    >>> env = mdb.Env('/tmp/mdbtest')
    >>> txn = env.begin_txn()
    >>> db = env.open_db(txn)
    >>> db.put(txn, 'hi', 'assinine')
    >>> txn.commit()
    >>> txn = env.begin_txn()
    >>> print '"%s"' % db.get(txn, 'hi')  # --> assinine
    >>> txn.close()
    >>> db.close()
    >>> env.close()

RELEASE NOTES:
0.2.6
    * Added integer values
    * Improved overall performance
