from pythreader import TaskQueue, Task, DEQueue

class DataLoaderTask(Task):
    
    def __init__(self, client, folder_name, data_keys, out_queue, context):
        Task.__init__(self)
        self.Client = client
        self.DataKeys = data_keys
        self.OutQueue = out_queue
        self.Context = context
        
    def run(self):
        for key, data in self.Client.get_many(self.Context, data_keys):
            self.OutQueue.append((key, data))
        self.OutQueue.append((None, None))

class UCDataStorageBase:
    # abstract base class
    
    def __init__(self, *params, **args):
        raise ValueError("UCDataStorageBase is an abstract class and can not be instantiated")
        
    def createFolder(self, name, owner, grants, drop_existing=False):
        pass
        
    def getData(self, folder_name, data_key):
        # returns BLOB or None
        return None
        
    def putData(self, folder_name, data):
        # returns data key, text
        return None     
    
    
    
    def getDataBulk(self, folder_name, data_keys):
        
        
        
        # generator of (key, BLOB) pairs for the list of data keys
        # if a key is not found, it will not be present in the output
        return None
    
