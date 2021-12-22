from ucondb.webapi import UConDBWebClient
import sys, getopt, json


Usage = """
python load_beam_data.py <server URL> <start run> <end run> <output directory>
"""

folder = "public.sp_protodune"
object = "configuration"

server_url, start_run, end_run, out_dir = sys.argv[1:]
start_run = int(start_run)
end_run = int(end_run)

client = UConDBWebClient(server_url)
versions = list(client.lookup_versions(folder, object, key_min=str(start_run), key_max=str(end_run+1)))
print(len(versions), " version received")

version_metas = {}      # version id -> meta

for v in versions:
    k = v.get("key")
    if k is not None:
        k = int(k)              # for ProtoDUNE, key is run number
        if k >= start_run and k <= end_run:
            version_metas[v["id"]] = v
            print("Found run #", k, "   version id=", v["id"])
            
open("ids.json","w").write(json.dumps(list(version_metas.keys())))

for vid, blob in client.get_versions_data(folder, version_metas.keys()):
    meta = version_metas[vid]
    run_number = int(meta["key"])
    print("run:", run_number, "   blob:", len(blob))
    f = open(out_dir + f"/info_{run_number}.fhicl", "wb")
    f.write(blob)
    f.close()



