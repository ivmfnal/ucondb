from getopt import getopt
from UConDB import UConDB
from UCon_psql import UCDPostgresDataStorage

import sys

Usage = """
python create_folder.py [options] <database name> [<namespace>.]<folder_name>
       
options:
    -h <host>
    -p <port>
    -U <user>
    -w <password>
    
    -c - force create, drop existing folder
    -o <table owner>
    -R <user>,...   - DB users to grant read permissions to
    -W <user>,...   - DB users to grant write permissions to
"""

host = None
port = None
user = None
password = None
grants_r = []
grants_w = []
drop_existing = False
owner = None

dbcon = []

opts, args = getopt(sys.argv[1:], 'h:U:w:p:co:R:W:')

if len(args) < 2 or args[0] == 'help':
    print(Usage)
    sys.exit(0)

opts = dict(opts)

if "-h" in opts:    dbcon.append("host=%s" % (opts["-h"],))
if "-p" in opts:    dbcon.append("port=%s" % (opts["-p"],))
if "-U" in opts:    dbcon.append("user=%s" % (opts["-U"],))
if "-w" in opts:    dbcon.append("password=%s" % (opts["-w"],))
drop_existing = "-c" in opts
grants_r = opts.get("-R","").split(",")
grants_w = opts.get("-W","").split(",")
owner = opts.get("-o")

dbcon.append("dbname=%s" % (args[0],))

dbcon = ' '.join(dbcon)
fname = args[1]

ds = UCDPostgresDataStorage(dbcon)
db = UConDB(dbcon, ds)

f = db.createFolder(fname, owner=owner, grants = {'r':grants_r, 'w':grants_w}, drop_existing=drop_existing)

print("Folder %s created" % (fname,))

