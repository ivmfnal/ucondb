from getopt import getopt
from UConDB import UConDB
from UCon_psql import UCDPostgresDataStorage

import sys

Usage = """
python put_object.py [options] <database name> <folder_name> <object_name>
options:

Database options:
    -h <host>
    -p <port>
    -U <user>
    -w <password>

    -f <input file name>        -- default stdin
    -t <tag>[,<tag>...]
    -T <valitity time>          -- integer or floating point Unix time, default=0
    -k <key>                    -- associate unique key with this version
"""

host = None
port = None
user = None
password = None
input_file = sys.stdin
key = None

tags = []
tv = 0

dbcon = []

opts, args = getopt(sys.argv[1:], 'h:U:w:p:f:T:t:k:')

if len(args) < 3 or args[0] == 'help':
    print(Usage)
    sys.exit(0)

for opt, val in opts:
    if opt == '-h':         dbcon.append("host=%s" % (val,))
    elif opt == '-p':       dbcon.append("port=%s" % (int(val),))
    elif opt == '-U':       dbcon.append("user=%s" % (val,))
    elif opt == '-w':       dbcon.append("password=%s" % (val,))
    elif opt == '-f':       input_file = open(val, "r")   
    elif opt == '-t':       tags = val.split(',')
    elif opt == '-T':       tv = float(val) 
    elif opt == '-k':       key = val

dbcon.append("dbname=%s" % (args[0],))

dbcon = ' '.join(dbcon)
fname = args[1]
oname = args[2]

ds = UCDPostgresDataStorage(dbcon)
db = UConDB(dbcon, ds)

f = db.getFolder(fname)
o = f.createObject(oname)
v = o.createVersion(input_file.read(), tv=tv, tags=tags, key=key)
print("Version %s created" % (v.ID))
