import mdb

env = mdb.Env('/tmp/crack', max_dbs=5)
txn = env.begin_txn()
db = env.open_db(txn)
db.put(txn, 'hi', 'ass')
db.put(txn, 'hi1', 'assinine')
db.put(txn, 'hi2', 'ass')
txn.commit()
txn = env.begin_txn()
print '"%s"' % db.get(txn, 'hi1')
for key, value in db.items(txn):
    print "key: %s value: %s" % (key, value)
txn.commit()
txn = env.begin_txn()
burst_data = {'a': 'helolo', 'zip':'zang', 'woody':'woodpecker'}
db.burst(txn, burst_data.iteritems(), True)
txn.commit()
txn = env.begin_txn()
for key, value in db.items(txn):
    print "key: %s value: %s" % (key, value)
txn.commit()
db.close()
env.close()
