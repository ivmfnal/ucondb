from datetime import datetime
from pythreader import PyThread, Primitive, Task, TaskQueue, DEQueue
from UCon_backend import UCDataStorageBase
from py3 import to_str, to_bytes
import requests

class UCDBlobStorage(UCDataStorageBase):

    def __init__(self, url, default_namespace=None, detect_duplicates = True):
        self.URL = url
        self.DetectDuplicates = detect_duplicates   # ignored for now
        self.DefaultNamespace = default_namespace   # ignored for now

    def createFolder(self, name, owner, grants, drop_existing=False):
        pass

    def putData(self, folder_name, data):        
        size = len(data)
        url = f"{self.URL}/blob?size={size}"
        response = requests.put(url, data=data)
        if respinse.status_code == 200:
            return response.text.strip()
        else:
            raise RuntimeError(f"HTTP error: {respinse.status_code}\n" + response.text)
                   
    def getData(self, folder_name, key):
        url = f"{self.URL}/blob?{key}"
        response = requests.get(url)
        if respinse.status_code == 200:
            return response.content
        else:
            raise RuntimeError(f"HTTP error: {respinse.status_code}\n" + response.text)

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

