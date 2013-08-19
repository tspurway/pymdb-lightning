import mdb
import time


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
                for _ in db.get_dup(txn, key):
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


if __name__ == "__main__":
    haikan_traverse()
