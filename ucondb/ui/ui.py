from ucondb import UConDBClient
import sys, getopt, os, json, time, pprint
from datetime import datetime

Usage = """
python ucondb.py [-s <server URL>] <command> ...

    -s <server URL>                     -- URL of the UConDB server. 
                                           Env. variable UCONDB_SERVER_URL can be used.
    
Commands:
    version                                       -- print server version information
    folders or ls                                 -- list folders
    objects or ls <folder>                        -- list objects in folder
    versions or ls [-t] [-j] <folder> <object>    -- list object versions
        -j                      -- JSON output for "versions"
        -t                      -- print Tv as date/time
    get <folder> <object> [get options] -- get object version metadata or data
        -m                      -- get metadata, print to stdout
        -k <key>                -- version key
        -i <id>                 -- version id
        -t <numeric>            -- version validity time, default = now
        -T <tag>                -- tag
        -R <numeric>            -- version which existed as of record time, default = now
        -o <output file>        -- output for the version data BLOB, otherwise - stdout
"""

def do_folders(client, args):
    _, args = getopt.getopt(args, "")
    folders = client.folders()
    #print(folders)
    for folder in folders:
        print(folder)
        
def do_objects(client, args):
    _, args = getopt.getopt(args, "")
    if not args:
        print(Usage)
        sys.exit(2)
        
    folder_name = args[0]
    for object_name in client.objects(folder_name):
        print(object_name)
        
def do_versions(client, args):
    opts, args = getopt.getopt(args, "jt")
    opts = dict(opts)

    if len(args) != 2:
        print(Usage)
        sys.exit(2)
    
    as_json = "-j" in opts
    as_datetime = "-t" in opts
    folder, object_name = args
    versions = client.lookup_versions(folder, object_name)
    if as_json:
        print("[", end="")
        begin = "\n"
        for version in versions:
            dump = json.dumps(version)
            print(begin+"  "+dump, end="")
            begin = ",\n"
        print("\n]")
    else:
        maxkeylen = max(len(v.get("key") or "") for v in versions)
        maxkeylen = max(3, maxkeylen)
        columns = ["id", "key", "Tr (UTC)", "Tv (UTC)" if as_datetime else "Tv", "Size"]
        clen = [10, max(3, maxkeylen), 19, 19, 9]
        data_format = f"%-10d %{maxkeylen}s %19s %19s %9d"
        cnames = [f"%-{n}s" % (cn,) for n, cn in zip(clen, columns)]
        print(" ".join(cnames))
        print(" ".join(["-"*l for l in clen]))
        for version in versions:
            tv = version["tv"]
            if as_datetime:
                tv = datetime.utcfromtimestamp(tv).strftime("%Y-%m-%d %H:%M:%S")
            else:
                tv = "%.3f" % (tv,)
            tr = datetime.utcfromtimestamp(version["tr"]).strftime("%Y-%m-%d %H:%M:%S")
            print(data_format % (version["id"], version.get("key") or "", tr, tv, version["data_size"]))
    
def do_get(client, args):
    folder, object_name, rest = args[0], args[1], args[2:]
    opts, _ = getopt.getopt(rest, "mk:i:R:t:T:o:")
    opts = dict(opts)
    key = opts.get("-k")
    vid = opts.get("-i")
    if vid: vid = int(vid)
    tr = opts.get("-R")
    if tr:  tr = float(tr)
    tv = opts.get("-t")
    if tv:  tv = float(tv)
    tag = opts.get("-T") 
    meta_only = "-m" in opts
    out = opts.get("-o")
    
    version_info = client.get_version(folder, object_name, tr=tr, tag=tag, tv=tv, key=key, id=vid, meta_only=meta_only)
    if version_info is None:
        print("Not found", file=sys.stderr)
        sys.exit(1)
    
    if meta_only:
        if "data" in version_info:
            del version_info["data"]
        pprint.pprint(version_info)
    else:
        if out:
            output = open(out, "wb")
        else:
            output = sys.stdout.buffer
        output.write(version_info["data"])
        
def do_version(client, args):
    print(client.version().strip())

def main():
    opts, args = getopt.getopt(sys.argv[1:], "s:")
    opts = dict(opts)

    if not args:
        print(Usage, file=sys.stderr)
        sys.exit(2)

    server_url = opts.get("-s") or os.environ.get("UCONDB_SERVER_URL")
    if not server_url:
        print("Server URL must be specified either with -s or as UCONDB_SERVER_URL environment variable", file=sys.stdout)
        sys.exit(2)

        
    client = UConDBClient(server_url)
    command, args = args[0], args[1:]
    if command == "ls":
        _, words = getopt.getopt(args, "jt")
        if not words:
            command = "folders"
        elif len(words) == 1:
            command = "objects"
        else:
            command = "versions"
    if command in ("ls", "objects"):
        return do_objects(client, args)
    elif command in ("lf", "folders"):
        return do_folders(client, args)
    elif command in ("lv", "versions"):
        return do_versions(client, args)
    elif command == "get":
        return do_get(client, args)
    elif command == "version":
        return do_version(client, args)
    else:
        print("Unknown command:", command, file=sys.stderr)
        print("\n"+Usage, file=sys.stderr)
        sys.exit(2)

if __name__ == "__main__":
    main()
        
    
    
        
