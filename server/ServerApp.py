from webpie import WPApp, WPHandler, app_synchronized
from webpie import Response as BaseResponse
import yaml
from UI import UConDBUIHandler
from wsdbtools import ConnectionPool

from ucondb import UConDB, Version
from ucondb.backends import UCDPostgresDataStorage

try:    from ucondb.backends import UCDCouchBaseDataStorage
except: UCDCouchBaseDataStorage = None      # cannot import couchbase ?

import time, sys, hashlib, os, random, threading, re, json
from datetime import datetime, timedelta, tzinfo
from ucondb.tools import text2datetime, epoch, fromepoch, to_bytes, to_str
from threading import RLock, Lock, Condition
import socket
from base64 import *
from hashlib import md5

from handler import UConDBHandler

class DBConfig(object):

    def __init__(self, cfg):
        self.Host = None
        self.DBName = cfg.get('Database','name')
        self.User = cfg.get('Database','user')
        self.Password = cfg.get('Database','password')
        self.DatsStoreType = cfg.get('ObjectStorage','type')
        #print "Object storage type: %s" % (self.DatsStoreType,)
        if self.DatsStoreType == 'couchbase':
            self.CouchbaseUsername = cfg.get('ObjectStorage','username')
            self.CouchbasePassword = cfg.get('ObjectStorage','password')
            self.CouchbaseBucket = cfg.get('ObjectStorage','bucket')
            self.CouchbaseURL = cfg.get('ObjectStorage','url')
            #print "Couchbase connection: %s %s" % (self.CouchbaseConn, self.CouchbasePassword)

        self.Port = cfg.get('Database','port')
        if self.Port is not None:
            self.Port = int(self.Port)
        self.Host = cfg.get('Database','host')
        self.Namespace = cfg.get('Database','namespace', 'public')
            
        connstr = "dbname=%s user=%s password=%s" % \
            (self.DBName, self.User, self.Password)
        if self.Port:   connstr += " port=%s" % (self.Port,)
        if self.Host:   connstr += " host=%s" % (self.Host,)

        self.ConnStr = connstr
        self.DB = None
        self.DataStore = None
        self.TableViewMap = {}

class DBConnection(object):
    
    def __init__(self, cfg):
        meta_cfg = cfg["Metadata"]
        connstr = "host=%(host)s port=%(port)s user=%(user)s dbname=%(dbname)s" % meta_cfg
        if meta_cfg.get("password"):
            connstr += " password=" + meta_cfg.get("password")
        self.MetaNamespace = meta_cfg.get("namespace") or "public"
        self.MetaConnPool = ConnectionPool(postgres=connstr, idle_timeout=5)
        
        data_cfg = cfg["Data"]
        self.DatsStoreType = data_storage_type = data_cfg["type"]
        if data_storage_type == 'couchbase':
            self.CouchbaseUsername = cfg.get('object_storage','username')
            self.CouchbasePassword = cfg.get('object_storage','password')
            self.CouchbaseBucket = cfg.get('object_storage','bucket')
            self.CouchbaseURL = cfg.get('object_storage','url')
            #print "Couchbase connection: %s %s" % (self.CouchbaseConn, self.CouchbasePassword)
        elif data_storage_type == 'kbs':
            self.KBSURL = cfg.get('object_storage','url')
            self.KBSUsername = cfg.get('object_storage','username')
            self.KBSPassword = cfg.get('object_storage','password')
        else:
            # postgres
            connstr = "host=%(host)s port=%(port)s user=%(user)s dbname=%(dbname)s" % data_cfg
            if data_cfg.get("password"):
                connstr += " password=" + data_cfg.get("password")
            self.DataNamespace = data_cfg.get("namespace") or "public"
            self.DataConnPool = ConnectionPool(postgres=connstr, idle_timeout=5)

    def ucondb(self):
        meta_db = self.MetaConnPool.connect()
        if self.DatsStoreType == 'couchbase':
            if self.DataStore is None:
                self.DataStore = UCDCouchBaseDataStorage(self.CouchbaseURL,
                        self.KBSUsername, self.CouchbasePassword, self.CouchbaseBucket)
            data_store = self.DataStore
        elif self.DatsStoreType == 'kbs':
            from UCon_kbs import UCDKBSDataStorage
            if self.DataStore is None:
                self.DataStore = UCDKBSDataStorage(self.KBSURL, self.KBSUsername, self.KBSPassword)
            data_store = self.DataStore            
        else:
            #Postgres
            data_db = self.DataConnPool.connect()
            data_store = UCDPostgresDataStorage(data_db, default_namespace=self.DataNamespace)
        return UConDB(meta_db, data_store, default_namespace=self.MetaNamespace)

    def disconnect(self):
        self.DataConnPool.close()
        self.MetaConnPool.close()

class DBConnection______:

    def __init__(self, cfg):
        self.Host = None
        self.DBName = cfg.get('Database','name')
        self.User = cfg.get('Database','user')
        self.Password = cfg.get('Database','password')
        self.DatsStoreType = cfg.get('ObjectStorage','type')
        #print "Object storage type: %s" % (self.DatsStoreType,)
        if self.DatsStoreType == 'couchbase':
            self.CouchbaseUsername = cfg.get('ObjectStorage','username')
            self.CouchbasePassword = cfg.get('ObjectStorage','password')
            self.CouchbaseBucket = cfg.get('ObjectStorage','bucket')
            self.CouchbaseURL = cfg.get('ObjectStorage','url')
            #print "Couchbase connection: %s %s" % (self.CouchbaseConn, self.CouchbasePassword)
        elif self.DatsStoreType == 'kbs':
            self.KBSURL = cfg.get('ObjectStorage','url')
            self.KBSUsername = cfg.get('ObjectStorage','username')
            self.KBSPassword = cfg.get('ObjectStorage','password')
        self.Port = cfg.get('Database','port')
        if self.Port is not None:
            self.Port = int(self.Port)
        self.Host = cfg.get('Database','host')
        self.Namespace = cfg.get('Database','namespace', 'public')
            
        connstr = "dbname=%s user=%s password=%s" % \
            (self.DBName, self.User, self.Password)
        if self.Port:   connstr += " port=%s" % (self.Port,)
        if self.Host:   connstr += " host=%s" % (self.Host,)
        
        self.ConnStr = connstr
        self.MetaDBPool = ConnectionPool(postgres = connstr, idle_timeout = 3)
        self.DataStore = None
        self.TableViewMap = {}
        
    def ucondb(self):
        if self.DatsStoreType == 'couchbase':
            if self.DataStore is None:
                self.DataStore = UCDCouchBaseDataStorage(self.CouchbaseURL,
                        self.KBSUsername, self.CouchbasePassword, self.CouchbaseBucket)
            data_store = self.DataStore
        elif self.DatsStoreType == 'kbs':
            from UCon_kbs import UCDKBSDataStorage
            if self.DataStore is None:
                self.DataStore = UCDKBSDataStorage(self.KBSURL, self.KBSUsername, self.KBSPassword)
            data_store = self.DataStore            
        else:
            #Postgres
            data_store = UCDPostgresDataStorage(self.ConnStr, default_namespace=self.Namespace)
        return UConDB(db, data_store, default_namespace=self.Namespace)

    def disconnect(self):
        self.DataStore = None
        
def as_datetime(x):
    if isinstance(x, (int, float)):
        x = fromepoch(x)
    return x.strftime("%Y-%m-%d %H:%M:%S%z")
    
class UConDBServerApp(WPApp):

    def __init__(self, *params, config_file=None):
        WPApp.__init__(self, *params)
        self.Version = Version
        self.Config = yaml.load(open(config_file or os.environ["UCONDB_CFG"], "r"), Loader = yaml.SafeLoader)
        self.DB = DBConnection(self.Config)
        self.DefaultNamespace = self.DB.MetaNamespace
        self.ServerConfig = self.Config["Server"]
        self.Title = self.ServerConfig.get("title")
        self.Authentication = self.ServerConfig.get('authentication', "RFC2617")
        assert self.Authentication in ("RFC2617", "none"), "Unknown authentication method: %s" % (self.Authentication,)
        
        self.ServerPassword = self.ServerConfig.get("password")

        self.Passwords = {}         # {"folder": {"user":"password",...},...}
        
        self.Passwords = self.Config.get("Authorization", {})
        """
        for f, lst in auth_config.items():
            lst = lst.split()
            lst = [tuple(x.split(':',1)) for x in lst]
            folder_dict = {}
            for u, p in lst:
                folder_dict[u] = p
            self.Passwords[f] = folder_dict
        """        
        self.ReadOnly = self.ServerConfig.get("read_only", False)

    def init(self):
        tempdirs=[self.ScriptHome]
        templates_dir = os.environ.get("JINJA2_TEMPLATES")
        if templates_dir is not None:
            tempdirs.insert(0, templates_dir)
        self.initJinjaEnvironment(tempdirs=tempdirs,
            filters = {
                "as_datetime": as_datetime
            },
            globals = {"GLOBAL_Title":self.Title, "GLOBAL_Version":Version}
        )

    def getPassword(self, folder, user):
        folder_dict = self.Passwords.get(folder, self.Passwords.get('*', {'*':None}))
        return folder_dict.get(user, folder_dict.get('*', None))
            
    def db(self):
        return self.DB.ucondb()
        
    def destroy(self):
        self.DB.disconnect()
        self.DB = None

def create_application(config_file=None):
    return UConDBServerApp(UConDBHandler, config_file=config_file)
        
application = create_application()

        
if __name__ == "__main__":
        application.run_server(8080)
