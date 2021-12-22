import psycopg2, sys, time
from datetime import datetime
#import cStringIO

#from trace import Tracer
from couchbase import FMT_BYTES
from couchbase.bucket import Bucket
from couchbase.cluster import Cluster, PasswordAuthenticator
from couchbase.exceptions import NotFoundError, TemporaryFailError
from UCon_backend import UCDataStorageBase

class UCDCouchBaseDataStorage(UCDataStorageBase):

    DATA_SIZE_LIMIT = 20*1000*1000      # Couchbase value size limit = 20 MB
    MAX_SINGLE_PIECE = DATA_SIZE_LIMIT/10
    MAX_CHUNK = MAX_SINGLE_PIECE - 1000  # we do not want to mess with too small chunks

    def __init__(self, url=None, username=None, password=None, bucket_name=None, bucket=None):
        assert (bucket is None) != (url is None or username is None or password is None or bucket_name is None), \
                        "Either bucket or (url, username, password, bucket_name) must be given"
        self.ClusterURL = url
        self.Password = password
        self.Username = username
        self.BucketName = bucket_name
        self.Bucket = bucket

    def connect(self):
        if not self.Bucket:
            self.Cluster = Cluster(self.ClusterURL)
            self.Cluster.authenticate(
                PasswordAuthenticator(self.Username, self.Password))
            self.Bucket = self.Cluster.open_bucket(self.BucketName)
        return self.Bucket

    def counterKey(self, folder_name):
        return "%s::id" % (folder_name,)
        
    def dataKey(self, folder_name, data_id):
        return "%s::%s" % (folder_name,data_id)
        
    def chunkKey(self, key, ichunk):
        return "%s.%05d" % (key, ichunk)
        
    def nextDataID(self, folder_name):
        b = self.connect()
        k = self.counterKey(folder_name)
        rv = b.counter(k)
        return rv.value
        
    def putData(self, folder_name, data):
        data_id = self.nextDataID(folder_name)
        key = self.dataKey(folder_name, data_id)
        b = self.connect()
        
        data_size = len(data)
        if data_size > self.MAX_SINGLE_PIECE:
            k = 0
            i = 0
            while i < data_size:
                j = i+self.MAX_CHUNK
                chunk = data[i:j]
                chunk_key = self.chunkKey(key, k)
                done = False
                while not done:
                    try:    rv = b.upsert(chunk_key, chunk, format=FMT_BYTES)
                    except TemporaryFailError:
                        pass
                    else:
                        done = True
                k += 1
                i = j
        else:
            rv = b.upsert(key, data, format=FMT_BYTES)
        #print data, rv
        return "%s" % (data_id,)
                   
    def _get_data(self, folder_name, data_id):
        key = self.dataKey(folder_name, data_id)
        b = self.connect()
        value = None            # not found
        try:    
            rv = b.get(key, no_format=True)
            value = rv.value
        except NotFoundError:
            # maybe it was split into chunks
            chunks = []
            k = 0
            done = False
            while not done:
                try:    
                    rv = b.get(self.chunkKey(key, k), no_format=True)
                    chunks.append(rv.value)
                except NotFoundError:   done = True
                k += 1
            if chunks:
                value = ''.join(chunks)
        return value

    def getData(self, folder_name, key_or_keys):
        if isinstance(key_or_keys, list):
            return ((key, self._get_data(folder_name, key)) for key in key_or_keys)
        else:
            return self._get_data(folder_name, key_or_keys)

        
    def createFolder(self, name, drop_existing=False):
        b = self.connect()
        k = self.counterKey(name)
        exists = True
        try:    x = b.get(k)
        except: exists = False
        if exists:
            if drop_existing:
                b.set(k, 0)
        else:
            b.set(k, 0)
                 

