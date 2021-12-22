import psycopg2, json, sys, io
import configparser

ids = json.load(open("ids.json", "r"))
print("loaded version ids:", len(ids))

config = configparser.ConfigParser()
config.read("ucondb.cfg")
dbhost = config.get("Database", "host")
dbport = config.get("Database", "port")
dbuser = config.get("Database", "user")
dbpassword = config.get("Database", "password")
dbname = config.get("Database", "name")

conn = psycopg2.connect(f"host={dbhost} port={dbport} user={dbuser} dbname={dbname} password={dbpassword}")
c = conn.cursor()

folder = "sp_protodune"

c.execute(f"""
    select distinct data_key from {folder}_versions where id=any(%s)
""", (ids,))

data_keys = [tup[0] for tup in c.fetchall()]
print("loaded data keys:", len(data_keys))


#            -- key >= {key_min} and key <= {key_max} and 


c.execute("show bytea_output;")
for line in c.fetchall():
    print(line)

query = """
    select key, data from %s_data
        where 
            key = any(array[%s])""" % (folder, ",".join(data_keys[:100]))

receiver = io.StringIO()

sql = f"""copy ( {query} ) to stdout with delimiter e'\\t' """
print(sql)
c.copy_expert(sql, receiver)
for l in receiver.getvalue().split("\n"):
    l = l.strip()
    if l:
        key, blob = l.strip().split("\t", 1)
        if blob.startswith(r"\\x"):
            blob = blob
            #blob = bytes.fromhex(blob[3:])
            
            #print("key:", key, f"blob:", repr(blob[:100]))
