import psycopg2, sys, time, zlib
from psycopg2.extensions import adapt, register_adapter, AsIs
from datetime import datetime
from pythreader import PyThread, Primitive, Task, TaskQueue, DEQueue
from wsdbtools import ConnectionPool

#import cStringIO

#from trace import Tracer
from ucondb.tools import DbDig, to_str, to_bytes
from .UCon_backend import UCDataStorageBase

def cursor_generator(c):
    while True:
        tup = c.fetchone()
        if tup is None:
            break
        yield tup
        
class DataLoaderTask(Task):
    
    def __init__(self, connection_pool, table_name, data_keys, out_queue):
        Task.__init__(self)
        self.ConnectionPool = connection_pool
        self.DataKeys = data_keys
        self.TableName = table_name
        self.OutQueue = out_queue
        
    def run(self):
        connection = self.ConnectionPool.connect()
        table_name = self.TableName
        c = connection.cursor()
        sql = f"""
            select key, data from {self.TableName}
                where key = any(%s)"""
        #print("getData: sql:", sql, "    keys:", key_or_keys)
        c.execute(sql, (self.DataKeys,))
        for key, data in cursor_generator(c):
            self.OutQueue.append((key, data))
        self.OutQueue.append((None, None))
        del connection


class UCDPostgresDataStorage(UCDataStorageBase):

    def __init__(self, conn_or_connstr, default_namespace=None, detect_duplicates = True):
        self.Conn = self.ConnStr = self.ConnPool = None
        if isinstance(conn_or_connstr, str):
            self.ConnStr = conn_or_connstr
            self.ConnPool = ConnectionPool(conn_or_connstr)
        elif isinstance(conn_or_connstr, ConnectionPool):
            self.ConnPool = conn_or_connstr
        else:
            self.Conn = conn_or_connstr
        self.DetectDuplicates = detect_duplicates
        self.DefaultNamespace = default_namespace

    def connect(self):
        if self.ConnPool is not None:
            return self.ConnPool.connect()
        else:
            return self.Conn
    
    def cursor(self):
        conn = self.connect()
        c = conn.cursor()
        if self.DefaultNamespace:
            c.execute(f"set schema '{self.DefaultNamespace}'")
        return c

    def tableName(self, folder_name):
        return "%s_data" % (folder_name,)

    def createFolder(self, name, owner, grants, drop_existing=False):
        c = self.cursor()
        table_name = self.tableName(name)
        exists = True
        try:    c.execute("select key from %s limit 1" % (table_name,))
        except:
            c.execute("rollback")
            exists = False
        if exists:
            if drop_existing:
                c.execute("drop table %s" % (table_name,))
            else:
                return

        if owner:
            c.execute("set role %s" % (owner,))

        words = table_name.split(".", 1)
        table_name_witout_namespace = table_name if len(words) == 1 else words[1]
        c.execute(f"""
            create table {table_name} (
                key     bigserial   primary key,
                hash    bigint,
                hash_type text default 'adler32',
                size    bigint,
                data    bytea);
            create index {table_name_witout_namespace}_hash_size_key on {table_name}(hash, size, key)""")
            
        read_roles = ','.join(grants.get('r',[]))
        if read_roles:
            c.execute(f"grant select on {table_name}, {table_name}_key_seq  to {read_roles}")
        write_roles = ','.join(grants.get('w',[]))
        if write_roles:
            c.execute(f"""
                    grant insert, delete, update on {table_name} to {write_roles};
                    grant all on {table_name}_key_seq to {write_roles}
            """)
        #c.execute("commit")
        

    def putData(self, folder_name, data):        
        data = to_bytes(data)
        a32 = zlib.adler32(data) & 0xFFFFFFFF
        l = len(data)
        #print("psql_backend.putData: data adler32, size:", a32, l)
        table_name = self.tableName(folder_name)
        key = None
        c = self.cursor()
        if self.DetectDuplicates:
            c.execute(f"""
                select key, data from {table_name}
                    where hash=%s and size=%s""", (a32, l))
            tup = c.fetchone()
            while tup is not None:
                k, d = tup
                #print("psql_backend: putData: data:", type(d), repr(d))
                if bytes(d) == data:
                    key = k
                    #print("putData: key found:", key)
                    break
                else:
                    #print("putData: data mismatch")
                    pass
                tup = c.fetchone()
        if not key:
                sql = f"""
                    insert into {table_name}(key, size, hash, data) values(default, %s, %s, %s)
                    returning key""" 
                c.execute(sql, (l, a32, psycopg2.Binary(data),))
                #c.execute(sql, (l, a32, data))
                key = c.fetchone()[0]
                c.execute("commit")
        return str(key)
                   
    def getData(self, folder_name, key):
        table_name = self.tableName(folder_name)
        c = self.cursor()
        sql = """
            select data from %s
                where key = %%s""" % (table_name,)
        #print "sql=<%s> key=%s" % (sql, long(key))
        c.execute(sql, (int(key),))
        tup = c.fetchone()
        if not tup:
            return None
        return bytes(tup[0])
    
    MAX_CONNECTIONS = 5
    KEYS_PER_TASK = 100
    OUT_QUEUE_SIZE = 100
    
    def getDataBulk(self, folder_name, keys):
        table_name = self.tableName(folder_name)
        assert self.ConnPool is not None
        
        task_queue = TaskQueue(self.MAX_CONNECTIONS)
        out_queue = DEQueue(capacity=self.OUT_QUEUE_SIZE)
        keys = [int(k) for k in keys]
        n = len(keys)
        for i in range(0, n, self.KEYS_PER_TASK):
            t = DataLoaderTask(self.ConnPool, table_name, keys[i:i+self.KEYS_PER_TASK], out_queue)
            task_queue << t
        
        while not task_queue.isEmpty():
            for key, data in out_queue:
                if key is None:
                    break
                yield str(key), data

