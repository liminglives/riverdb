from riverdb import *


def test_dump_from_dataframe():
    df = pandas.read_csv("t.csv")
    print df
    RiverDB.dump_from_dataframe("t.dump", df)

#b = csv_binary.reader("rtt.large.binary3")
#print b.col_num()
#print b.row_num()
#print b.index()

def test_pandas(f):
    print "test pandas"
    import time
    import datetime
    datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')
    start = time.time();
    print "read pandas"
    df = RiverDB.to_dataframe(f)
    print df
    print "pandas cost:", time.time() - start

def test_writer(f):
    import time
    import datetime
    datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')
    start = time.time();
    w = writer(f)
    header = [("AA",2), ("BB",7), ("CC",10)]
    w.write_header(header)
    w.write_row([1231, 1.222, ""])
    w.write_row([123, None, "acbsjj"])
    w.write_row([12, 1.2224, None])
    w.flush()
    w.close()
    print "writer cost:", time.time() - start

def test_reader(f):
    import time
    import datetime
    datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')
    start = time.time();
    r = reader(f)
    for i in r:
        print i
    r.close()
    print "reader cost:", time.time() - start

def test_dictreader(f):
    reader = DictReader(f)
    for row in reader:
        print row
    reader.close()

def test_dictwriter(f):
    w = DictWriter(f)
    header = [("AA",RiverDB.INT16), ("BB",RiverDB.FLOAT), ("CC",RiverDB.STRING)]
    w.write_header(header)
    w.write_row({"AA":1231, "BB":1.222, "CC":"aas"})
    w.write_row({"AA":121, "CC":"aasasfs"})
    w.write_row({"AA":31, "BB":5.222})
    w.flush()
    w.close()
    w.close()

#f = "rtt.large.binary3"
f = "w.binary"
#f = "rtt.csv.binary.gz"
#f = "t.dump.gz"
#test_writer(f)
#test_pandas(f)
#test_reader(f)
#test_dump_from_dataframe()
#test_dictwriter(f)
test_dictreader(f)
print f

#print df


