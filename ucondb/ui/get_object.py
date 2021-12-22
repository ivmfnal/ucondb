from getopt import getopt
from UConDB import UConDB
from UCon_psql import UCDPostgresDataStorage

import sys, time

Usage = """
python put_object.py [options] <database name> <folder_name> <object_name>
options:

Database options:
    -h <host>
    -p <port>
    -U <user>
    -w <password>

    -m                          -- only show metadata
    -f <output file name>       -- default stdout
    -t <tag>
    -T <valitity time>          -- integer or floating point Unix time, default=now
    -k <key>                    -- get version by key - -t and -T will be ignored
"""

host = None
port = None
user = None
password = None
output_file = None
meta_only = False
key = None

tag = None
tv = time.time()

dbcon = []

opts, args = getopt(sys.argv[1:], 'h:U:w:p:f:T:t:mk:')

if len(args) < 3 or args[0] == 'help':
    print(Usage)
    sys.exit(0)

for opt, val in opts:
    if opt == '-h':         dbcon.append("host=%s" % (val,))
    elif opt == '-p':       dbcon.append("port=%s" % (int(val),))
    elif opt == '-U':       dbcon.append("user=%s" % (val,))
    elif opt == '-w':       dbcon.append("password=%s" % (val,))
    elif opt == '-f':       output_file = open(val, "wb")   
    elif opt == '-t':       tags = val.split(',')
    elif opt == '-T':       tv = float(val) 
    elif opt == '-m':       meta_only = True
    elif opt == '-k':       key = val

dbcon.append("dbname=%s" % (args[0],))

dbcon = ' '.join(dbcon)
fname = args[1]
oname = args[2]

ds = UCDPostgresDataStorage(dbcon)
db = UConDB(dbcon, ds)

f = db.getFolder(fname)
o = f.getObject(oname)
v = o.getVersion(tag=tag, tv=tv, key=key)
if meta_only:
    print("Version id: ", v.ID)
    print("Tv:         ", v.Tv)
    print("Tr:         ", v.Tr)
    print("Tags:       ", ','.join(v.getTags()))
    print("Data key:   ", v.DataKey)
else:
    if output_file is None:
        sys.stdout.write(v.Data.decode("utf-8", "ignore"))
    else:
        output_file.write(v.Data)

