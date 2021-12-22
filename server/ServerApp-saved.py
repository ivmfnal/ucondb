from WSGIApp import WSGIApp, WSGISessionApp, WSGIHandler, Request, Application
from WSGIApp import Response as BaseResponse
from  configparser import ConfigParser
from UConDB import UConDB, Signature
from UI import UConDBUIHandler

from UCon_psql import UCDPostgresDataStorage

try:    from UCon_couchbase import UCDCouchBaseDataStorage
except: UCDCouchBaseDataStorage = None      # cannot import couchbase ?

import time, sys, hashlib, os, random
from datetime import datetime, timedelta, tzinfo
from timelib import text2datetime, epoch
from threading import RLock, Lock, Condition
import threading, re, json
import socket

from base64 import *
import md5

from Version import Version


def md5sum(data):
  m = md5.new()
  m.update(data)
  return m.hexdigest()

class Response(BaseResponse):

    def __init__(self, *params, **agrs):
        BaseResponse.__init__(self, *params, **agrs)
        self.headers.add("X-Actual-Server", socket.gethostname())
        self.headers.add("X-Server-Application-Version", "UConDB %s" % (Version,))
        self.headers.add("Access-Control-Allow-Origin", "*")

def synchronized(method):
    def smethod(self, *params, **args):
        self._Lock.acquire()
        try:    
            return method(self, *params, **args)
        finally:
            self._Lock.release()
    return smethod

class   ConfigFile(ConfigParser):
    def __init__(self, request, path=None, envVar=None):
        ConfigParser.__init__(self)
        if not path:
            path = request.environ.get(envVar, None) or os.environ[envVar]
        self.read(path)


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

        self.Port = None
        try:    
            self.Port = int(cfg.get('Database','port'))
        except:
            pass
            
        self.Host = None
        try:    
            self.Host = cfg.get('Database','host')
        except:
            pass
            
        self.Namespace = 'public'
        try:    
            self.Namespace = cfg.get('Database','namespace')
        except:
            pass
            
        connstr = "dbname=%s user=%s password=%s" % \
            (self.DBName, self.User, self.Password)
        if self.Port:   connstr += " port=%s" % (self.Port,)
        if self.Host:   connstr += " host=%s" % (self.Host,)

        self.ConnStr = connstr
        self.DB = None
        self.DataStore = None
        self.TableViewMap = {}
        
    def connection(self):
        if self.DB == None:
            if self.DatsStoreType == 'couchbase':
                self.DataStore = UCDCouchBaseDataStorage(self.CouchbaseURL, 
                        self.CouchbaseUsername, self.CouchbasePassword, self.CouchbaseBucket)
            else:
                self.DataStore = UCDPostgresDataStorage(self.ConnStr)
            self.DB = UConDB(self.ConnStr, self.DataStore)
        return self.DB

    def disconnect(self):
        self.DataStore = None
        if self.DB:
            self.DB.disconnect()
            self.DB = None


StaticDBConnection = None
ConnectionLock = RLock()

def createDBConnection(config):
    global StaticDBConnection
    with ConnectionLock:
        if StaticDBConnection is None:
            StaticDBConnection = DBConnection(config)
        return StaticDBConnection
        
class UConDBServerApp(WSGIApp):

    def __init__(self, request, rootclass):
        WSGIApp.__init__(self, request, rootclass)
        self.Config = ConfigFile(request, envVar = 'UCONDB_CFG')
        self.DB = createDBConnection(self.Config)
        try:    self.Authentication = self.Config.get('Server','authnetication')
        except: self.Authentication = "RFC2617"
        assert self.Authentication in ("RFC2617", "none")
        
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
    
    def init(self, root):
        self.initJinja2(tempdirs=[os.path.dirname(__file__)],
            filters = {
            }
        )

    def getPassword(self, folder, user):
        folder_dict = self.Passwords.get(folder, self.Passwords.get('*', {'*':None}))
        return folder_dict.get(user, folder_dict.get('*', None))
            
    def db(self):
        return self.DB.connection()
        
    def destroy(self):
        self.DB.disconnect()
        self.DB = None
        
    def dblock(self):
        return ConnectionLock
        

class UConDBHandler(WSGIHandler):
    def __init__(self, req, app):
        WSGIHandler.__init__(self, req, app)
        self.UI = UConDBUIHandler(req, app)
        #self.DB = app.db()
        
    def index(self, req, relpath, **args):
        head = req.application_url
        out = """
            <html>
            <head>
                <style type="text/css">
                    p, body, td, tr {
                        font-family: arial, verdana;
                        }
                </style>
            </head>
            <body>
            <p>Data access URL head:<p>
            <p style="margin-left:50px">%(u)s</p>
            <p>E.g.: <a href="%(u)s/folders">%(u)s/folders</a></p>
            <p>See <a href="https://cdcvs.fnal.gov/redmine/projects/ucondb/wiki/Proposal" target="_blank">documentation</a></p>
            </body>
            </html>""" % {'u':head}
        return Response(out)
            
    def probe(self, req, relpath, **args):
        with self.App.dblock():
            try:    
                c = self.App.db().cursor()
                c.execute("select 1")
                tup = c.fetchone()
                if tup[0] == 1:
                    return Response("OK")
                else:
                    raise ValueError("Data mismatch. Expected (1,), got %s" % (tup,))
            except:
                return Response("Error: %s %s" % (sys.exc_info()[0], sys.exc_info()[1]))

    def hello(self, req, relpath, **args):
        #print req
        return Response("hello: x=%s" % (req.GET.get('x'),))
        
    def authenticateSignature(self, folder, req, data):
        try:
            authenticator = req.headers['X-UConDB-Authenticator']
        except KeyError:
            return False
        if not authenticator:  return False

        def get_password(user):
            return self.App.ServerPassword if user == "*" else self.App.getPassword(folder, user)

        ok = Signature(folder, data).verify(authenticator, get_password)
        return ok

    def digestAuthorization(self, folder, env, body):
        #print "authorizing..:"
        realm = "%s.ucondb" % (folder, )
        auth_header = env.get("HTTP_AUTHORIZATION","")
        matches = re.compile('Digest \s+ (.*)', re.I + re.X).match(auth_header)
        header = ""
        ok = False
        if matches:
            vals = re.compile(', \s*', re.I + re.X).split(matches.group(1))

            dict = {}

            pat = re.compile('(\S+?) \s* = \s* ("?) (.*) \\2', re.X)
            for val in vals:
                ms = pat.match(val)
                if ms:
                    dict[ms.group(1)] = ms.group(3)

            user = dict['username']
            cfg_password = self.App.getPassword(folder, user)
            if cfg_password == None:
                return False, None
                    
            a1 = md5sum('%s:%s:%s' % (user, realm, cfg_password))        
            a2 = md5sum('%s:%s' % (env['REQUEST_METHOD'], dict["uri"]))
            myresp = md5sum('%s:%s:%s:%s:%s:%s' % (a1, dict['nonce'], dict['nc'], dict['cnonce'], dict['qop'], a2))
            #print "response:   ", dict['response']
            #print "my response:", myresp
            ok = myresp == dict['response']
        else:
            #print "no matches"
            pass
        if not ok:
            nonce = b64encode(str(int(time.time())))
            header = 'Digest realm="%s", nonce="%s", algorithm=MD5, qop="auth"' % (realm, nonce)
        return ok, header

        
    def parseArgs(self, relpath, args):
        d = {}
        if relpath:
            for w in relpath.split('/'):
                kv = tuple(w.split('=',1))
                if len(kv) > 1:
                    k, v = kv
                else:
                    if "folder" in d:
                        k = "object"
                    else:
                        k = "folder"
                    v = kv[0]
                if k in d:
                    raise ValueError("Value for %s specified twice in the URL" % (k,))
                d[k] = v

        for k, v in list(args.items()):
            if k in d:
                raise ValueError("Value for %s specified twice in the URL" % (k,))
            d[k] = v

        for v in list(d.values()):
            if "'" in v or ";" in v:
                raise ValueError("Invalid argument value")
            
        return d

    def authenticate(self, req, folder):
        body = req.body
        if 'X-UConDB-Authenticator' in req.headers:
            if not self.authenticateSignature(folder, req, body):
                return False, Response("Authentication falied", status=401)
        elif self.App.Authentication == "RFC2617":         
            #print folder.Name
            ok, header = self.digestAuthorization(folder, req.environ, body)
            if not ok:
                resp = Response("Authorization required", status=401)
                if header:
                    resp.headers['WWW-Authenticate'] = header
                return False, resp
        return True, None

    def doPut(self, req, params):    
        with self.App.dblock():
            folder = params.get("folder")
            folder = self.App.db().getFolder(folder)
            ok, resp = self.authenticate(req, folder.Name)
            if not ok:  return resp         # authentication failrure
            tags = params.get("tag", [])
            object = params.get("object")
            tv = params.get("tv", None)
            if tv:  tv = float(tv)
            key = params.get("key")
            override = params.get("override", "no") == "yes"
            o = folder.createObject(object)
            if type(tags) == type(""):
                tags = [tags]
            #print "Request body:", req.body
            v = o.createVersion(req.body, tv, tags=tags, key=key, override_key=override)
            return Response(str(v.ID))

    def put(self, req, relpath, **args):  
        params = self.parseArgs(relpath, args)
        return self.doPut(req, params)        
        
    def doGet(self, req, params):
        with self.App.dblock():
            folder = params.get("folder")
            object = params.get("object")
            version_id = params.get("version_id")
            tr = params.get("tr")
            if tr:  tr=text2datetime(tr)
            tv = params.get("tv", time.time())
            if tv:  tv = float(tv)
            tag = params.get("tag")
            key = params.get("key")
            meta_only = params.get("meta_only", "no")
            if version_id != None:
                return self.dataByVersionID(folder, version_id, meta_only)
            o = self.App.db().getFolder(folder).getObject(object)
            if key != None:
                v = o.getVersion(key=key)
            else:
                if tag != None:
                    v = o.getVersion(tag=tag)
                else:
                    v = o.getVersion(tv=tv, tr=tr)
            if meta_only == "yes":
                reply = {}
                if v:
                    reply = {
                        "Tv":   v.Tv,
                        "Tr":   epoch(v.Tr),
                        "Tags": v.getTags(),
                        "Adler32":  v.Adler32,
                        "UAdler32": v.Adler32 & 0xFFFFFFFF,
                        "DataSize": v.DataSize,
                        "ID":   v.ID,
                        "Key":  v.Key
                        }
                ret = Response(json.dumps(reply), content_type="text/json")
            else:
                ret = Response(v.Data, content_type="text/plain")
            return ret

    def get(self, req, relpath, **args):  
        #print "relpath=", relpath
        params = self.parseArgs(relpath, args)
        return self.doGet(req, params)        
        
        
    def data(self, req, relpath, **args):
        params = self.parseArgs(relpath, args)
        if req.method.lower() in ("put", "post"):
            return self.doPut(req, params)
        else:
            return self.doGet(req, params)
            
    def dataByVersionID(self, folder, version_id, meta_only):
        with self.App.dblock():
            version_id = int(version_id)
            meta_only = meta_only == "yes"
            folder = self.App.db().getFolder(folder)
            v = folder.getVersionByID(version_id)
            if meta_only:
                reply = {}
                if v:
                    reply = {
                        "Tv":   v.Tv,
                        "Tr":   epoch(v.Tr),
                        "Tags": v.getTags(),
                        "Adler32":  v.Adler32,
                        "UAdler32": v.Adler32 & 0xFFFFFFFF,
                        "DataSize": v.DataSize,
                        "Key":  v.Key,
                        "ID":   v.ID
                        }
                ret = Response(json.dumps(reply))
            else:
                ret = Response(v.Data)
            return ret
        
    def objects(self, req, relpath, folder=None, format="json", **args):
        with self.App.dblock():
            folder = self.App.db().getFolder(folder)
            objects = folder.listObjects()
            if format == "json":
                out = json.dumps([o.Name for o in objects])
            else:
                # CSV
                out = "Name\n" + "\n".join([o.Name for o in objects])
            return Response(out, content_type="text/"+format)        
        
    def tags(self, req, relpath, folder=None, format="json", **args):
        with self.App.dblock():
            folder = self.App.db().getFolder(folder)
            tags = folder.listTags()
            if format == "json":
                out = json.dumps(tags)
            else:
                # CSV
                out = "Name\n" + "\n".join(tags)
            return Response(out, content_type="text/"+format)   

    def create_folder(self, req, relpath, folder=None, owner=None, 
                            read=None, write=None, drop="no", **args):
        with self.App.dblock():
            if not folder:
                return Response("Empty folder name", status=400)
            ok, resp = self.authenticate(req, folder)
            if not ok:
                return resp
            drop = drop == "yes"
            read_list = [] if not read else read.split(",")
            write_list = [] if not write else write.split(",")
            db = self.App.db()
            db.createFolder(folder, owner=owner, grants = {'r':read_list, 'w':write_list },
                    drop_existing=drop)
            return Response('OK')
               
        
    def folders(self, req, relpath, format="json", namespace="public", **args):  
        with self.App.dblock():
            folders = [f.Name for f in self.App.db().listFolders(namespace)]
            if format == "json":
                out = json.dumps(folders)
            else:
                # CSV
                out = "Name\n" + "\n".join(folders)
            return Response(out, content_type="text/"+format)   
        
    def versions(self, req, relpath, folder=None, object=None, format="json", namespace="public", tv=None, tr=None, tr_since=None, **args):
        if tv is None:  
            tv = time.time()
        else:
            tv = float(tv)
        if tr is not None:  tr = text2datetime(tr)
        if tr_since is not None:  tr_since = text2datetime(tr_since)
        
        with self.App.dblock():
            o = self.App.db().getFolder(folder).getObject(object)
            versions = o.listVersions(tr=tr, tv=tv, tr_since=tr_since)
        dicts = [       
                {
                    "Tv":   v.Tv,
                    "Tr":   epoch(v.Tr),
                    "Tags": v.getTags(),
                    "Adler32":  v.Adler32,
                    "UAdler32": v.Adler32 & 0xFFFFFFFF,
                    "DataSize": v.DataSize,
                    "Key":  v.Key,
                    "ID":   v.ID
                } for v in versions]
        return Response(json.dumps(dicts), content_type="text/json")   
                    
                    
application = Application(UConDBServerApp, UConDBHandler)
        
       
        



