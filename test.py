import mdb

env = mdb.Env('/tmp/mdbtest', max_dbs=5)
txn = env.begin_txn()
db = env.open_db(txn)
db.put(txn, 'hi2', 'asshat')
db.put(txn, 'hi', 'assissippi')
db.put(txn, 'hi1', 'assinine')
txn.commit()
txn = env.begin_txn()
print '"%s"' % db.get(txn, 'hi1')
for key, value in db.items(txn):
    print "key: %s value: %s" % (key, value)
txn.commit()
db.close()
env.close()
