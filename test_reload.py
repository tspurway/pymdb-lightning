import mdb
import ujson
from datetime import datetime
from gevent import sleep
import csv
import time


# a compact csv dialect which escapes ',' properly
csv.register_dialect('escaped',
                     escapechar='\\',
                     doublequote=False,
                     quoting=csv.QUOTE_NONE,
                     lineterminator='\n'
                     )


def reload():
    nround = 0
    while 1:
        env = mdb.Env('/var/cache/bidder/haikan/keyword_chest',
                      mapsize=10 * mdb.GB)
        txn = env.begin_txn()
        db = env.open_db(txn)
        start = datetime.now()
        db.drop(txn)
        print "Time elapsed to drop: %d seconds" % (datetime.now() - start).seconds
        nround += 1
        done = failed = total = counter = 0
        start = datetime.now()
        with open('/home/chango/haikan/keyword_chest', 'r') as f:
            reader = csv.reader(f, dialect="escaped")
            fieldnames = reader.next()
            try:
                counter, done, failed = 0, 0, 0
                start = datetime.now()
                for line in reader:
                    counter += 1
                    row = dict(zip(fieldnames, line))
                    key = ' '.join([row[k] for k in ('phrase',)])
                    value = ujson.dumps(row)
                    if len(value) >= 512 or len(key) >= 512:
                        failed += 1
                        print("Skipped a key/value due to its length")
                        continue
                    db.put(txn, key, value)
                    done += 1
                    if counter >= 300000:
                        txn.commit()
                        txn = env.begin_txn()
                        counter = 0
            except Exception as e:
                print("Error when building %s" % e)
                failed += 1
                txn.abort()
            else:
                txn.commit()
            finally:
                db.close()
                env.close()
        print "Round %d completed, %d scanned. %d were done, %d failed" % (nround, total, done, failed)
        print "Time elapsed: %d seconds" % (datetime.now() - start).seconds
        sleep(1)
        break


def reload_again():
    import lmdb
    nround = 0
    while 1:
        env = lmdb.open('/var/cache/bidder/haikan/keyword_chest',
                        map_size=10 * mdb.gb, max_dbs=8, sync=False)
        nround += 1
        done = failed = total = counter = 0
        start = datetime.now()
        with open('/home/chango/haikan/keyword_chest', 'r') as f:
            reader = csv.reader(f, dialect="escaped")
            fieldnames = reader.next()
            counter, done, failed = 0, 0, 0
            start = datetime.now()
            txn = lmdb.transaction(env)
            start = datetime.now()
            db = env.open_db(txn, dupsort=True)
            txn.drop(db, delete=False)
            print "time elapsed to drop: %d seconds" % (datetime.now() - start).seconds
            for line in reader:
                counter += 1
                row = dict(zip(fieldnames, line))
                key = ' '.join([row[k] for k in ('phrase',)])
                value = ujson.dumps(row)
                if len(value) >= 512 or len(key) >= 512:
                    failed += 1
                    print("skipped a key/value due to its length")
                    continue
                txn.put(key, value)
                done += 1
                if counter >= 300000:
                    txn.commit()
                    txn = lmdb.transaction(env)
                    counter = 0
            txn.commit()
            env.close()
        print "round %d completed, %d scanned. %d were done, %d failed" % (nround, total, done, failed)
        print "time elapsed: %d seconds" % (datetime.now() - start).seconds
        sleep(1)
        break


def dump_file():
    f_out = open("dumps", "w")
    with open('/home/chango/haikan/keyword_chest/keyword_chest', 'r') as f:
        reader = csv.reader(f, dialect="escaped")
        fieldnames = reader.next()
        for line in reader:
            row = dict(zip(fieldnames, line))
            key = ' '.join([row[k] for k in ('phrase',)])
            value = ujson.dumps(row)
            if len(value) >= 512 or len(key) >= 512:
                print("skipped a key/value due to its length")
                continue
            f_out.write("%s,%s\n" % (key, value))
    f_out.close()


def haikan_traverse():
    import csv
    # a compact csv dialect which escapes ',' properly
    csv.register_dialect('escaped',
                         escapechar='\\',
                         doublequote=False,
                         quoting=csv.QUOTE_NONE,
                         lineterminator='\n'
                         )
    while 1:
        env = mdb.Env('/var/cache/haikan/keyword_chest', flags=mdb.MDB_RDONLY)
        txn = env.begin_txn(flags=mdb.MDB_RDONLY)
        db = env.open_db(txn, flags=mdb.MDB_DUPSORT)
        with open("/home/chango/haikan/keyword_chest", 'r') as in_file:
            # Do NOT use DictReader for performance reason
            reader = csv.reader(in_file, dialect="escaped")
            fieldnames = reader.next()
            scanned_keys = set()
            counter, done = 0, 0
            start = time.time() * 1e6
            for line in reader:
                row = dict(zip(fieldnames, line))
                key = row["phrase"]
                counter += 1
                if key in scanned_keys:
                    continue
                scanned_keys.add(key)
                for key_, _ in db.get_dup(txn, key):
                    done += 1
                if done >= 1500:
                    txn.abort()
                    env.close()
                    end = time.time() * 1e6
                    print "Scanned %d items, time elapsed: %d" % (done, int(end - start))
                    env = mdb.Env('/var/cache/haikan/keyword_chest', flags=mdb.MDB_RDONLY)
                    txn = env.begin_txn(flags=mdb.MDB_RDONLY)
                    db = env.open_db(txn, flags=mdb.MDB_DUPSORT)
                    done = 0
                    time.sleep(2)


if __name__ == '__main__':
    #reload()
    #haikan_traverse()
    dump_file()
