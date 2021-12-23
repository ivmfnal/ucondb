from webpie import WPApp, WPHandler, app_synchronized
from webpie import Response as BaseResponse
from  configparser import ConfigParser
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

class   ConfigFile(object):
    def __init__(self, path=None, envVar=None):
        path = path or os.environ.get(envVar)
        self.Config = ConfigParser()
        if path:
            self.Config.read(path)
        
    def get(self, section, param, default=None):
        try:    return self.Config.get(section, param)
        except: return default

    def __getattr__(self, attr):
        return getattr(self.Config, attr)
        
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
        
class DBConnection:

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
        db = self.MetaDBPool.connect()
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
        self.Config = ConfigFile(path=config_file, envVar = 'UCONDB_CFG')
        self.Title = self.Config.get("Server","title")
        self.DB = DBConnection(self.Config)
        self.DefaultNamespace = self.DB.Namespace
        self.Authentication = self.Config.get('Server','authentication', "RFC2617")
        assert self.Authentication in ("RFC2617", "none"), "Unknown authentication method: %s" % (self.Authentication,)
        
        self.ServerPassword = None
        try:    
            self.ServerPassword = self.Config.get('Server','password')
        except: pass

        self.Passwords = {}         # {"folder": {"user":"password",...},...}
        
        if self.Config.has_section("Authorization"):
            for f, lst in self.Config.items("Authorization"):
                lst = lst.split()
                lst = [tuple(x.split(':',1)) for x in lst]
                folder_dict = {}
                for u, p in lst:
                    folder_dict[u] = p
                self.Passwords[f] = folder_dict
                
        self.ReadOnly = self.Config.get("Server", "read_only", False) == "yes"
    
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
