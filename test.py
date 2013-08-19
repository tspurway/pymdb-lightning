# -*- coding: utf-8 -*-
import mdb

env = mdb.Env('./testdbm', max_dbs=5)
txn = env.begin_txn()
db = env.open_db(txn)
#db.put(txn, 'hi', 'assis')
#db.put(txn, 'hi', 'assissippi')
db.put(txn, 'hi1', 'assinine')
db.put(txn, 'hii1\0a', 'assinine')
db.put(txn, 'hi2', u'aΩ\u03a8shat'.encode('utf-8'))
db.put(txn, 'hi2', u'aΩ\u03a9shat'.encode('utf-8'))
txn.commit()
txn = env.begin_txn()
print '"%s"' % db.get(txn, 'hi1')
print '"%s"' % db.get(txn, 'hi2')
print '"%s"' % db.get(txn, 'hii1\0a')

db.delete(txn, 'hi1', 'assinine')
try:
    db.get(txn, 'hi1')
except Exception as e:
    print e

print "\ntest for traversal"
for key, value in db.items(txn):
    print "key: %s value: %s" % (key, value)

print "\ntest for duplicate retrieval"
for value in db.get_dup(txn, 'hi2'):
    print "value: %s" % value

txn.commit()
db.close()
env.close()
