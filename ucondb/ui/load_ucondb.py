import getopt, sys

PY3 = sys.version_info >= (3,)
if PY3:
        from urllib.request import urlopen
else:
        from urllib2 import urlopen

Usage = """
load_ucondb.py [options] <URL head>
Options:
    -f <folder>
    -o <object>
    -s <size>
    -N <nfiles>
    -k <key head>
"""

DataSize = 100000
N = 100
KeyHead = None
URLHead = None
Folder = "test"
Object = "test"

opts, args = getopt.getopt(sys.argv[1:], "N:s:k:")

for opt, val in opts:
    if opt == '-N':     N = int(val)
    elif opt == '-s':   DataSize = int(val)
    elif opt == '-k':   KeyHead = val
    elif opt == '-f':   Folder = val
    elif opt == '-o':   Object = val
    else:
        print(Usage)
        sys.exit(0)

URLHead = args[0]
        
for i in range(N):
    data = "%010d" % (i,)
    data = data * (DataSize/len(data))
    url = URLHead + "/put?folder=%s&object=%s" % (Folder, Object)
    if KeyHead:
        url += "&key=%s%010d" % (KeyHead, i)
    resp = urlopen(url, data)
    print(resp.getcode(), resp.read())
    
    
