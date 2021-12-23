from webpie import WPHandler, app_synchronized
from webpie import Response as BaseResponse
from ucondb import UConDB, Signature, Version
from UI import UConDBUIHandler
import json, zlib, re
from wsdbtools import ConnectionPool

from ucondb.backends import UCDPostgresDataStorage

try:    from ucondb.backends import UCDCouchBaseDataStorage
except: UCDCouchBaseDataStorage = None      # cannot import couchbase ?

import time, sys, hashlib, os, random, threading, re, json
from datetime import datetime, timedelta, tzinfo

import socket
import rfc2617
from base64 import *
from hashlib import md5

from ucondb.tools import to_bytes, to_str, text2datetime, epoch, fromepoch
from urllib.parse import unquote

class ObjectNotFoundError(Exception):
    
    def __init__(self, folder, object):
        self.Folder = folder
        self.Object = object
        
    def __str__(self):
        return f"Object {self.Object} not found in folder {self.Folder}"

class FolderNotFoundError(Exception):
    
    def __init__(self, folder):
        self.Folder = folder
        
    def __str__(self):
        return f"Folder {self.Folder} not found"
        
class ParameterError(ValueError):
    pass
    
class Response(BaseResponse):

    def __init__(self, *params, **agrs):
        BaseResponse.__init__(self, *params, **agrs)
        self.headers.add("X-Actual-Server", socket.gethostname())
        self.headers.add("X-Server-Application-Version", "UConDB %s" % (Version,))
        self.headers.add("Access-Control-Allow-Origin", "*")

def stream_as_json_list(iterable):
    yield "["
    first = True
    for x in iterable:
        # assume x is JSON object representation
        if not first:
            yield ", "
        yield(x)
        first = False
    yield "]"

def stream_in_chunks(data_source, chunk=16*1024):
    parts = []
    chunk_size = 0
    total_size = 0

    for part in data_source:
        if part:
            parts.append(part)
            chunk_size += len(part)
        if chunk_size >= chunk:
            yield "".join(parts)
            total_size += chunk_size
            chunk_size = 0
            parts = []

    if parts:
        yield "".join(parts)
        total_size += len("".join(parts))
    
    #print("stream_in_chunks: total_size=", total_size)
        
RS = "\x1E"

def stream_as_json_seq(iterable, chunk=1024*1024):
    return stream_in_chunks(("\x1E" + json.dumps(x) + "\n" for x in iterable), chunk)

class UConDBHandler(WPHandler):
    def __init__(self, *params):
        WPHandler.__init__(self, *params)
        self.UI = UConDBUIHandler(*params)
        #self.DB = app.db()
        
    def index(self, req, relpath, **args):
        self.redirect("./UI/index")

    def version(self, req, relpath, **args):
        return self.App.Version + "\n"

    def probe(self, req, relpath, **args):
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
            ok, header = rfc2617.digest_server(folder, req.environ, self.App.getPassword)
            if not ok:
                resp = Response("Authorization required", status=401)
                if header:
                    resp.headers['WWW-Authenticate'] = header
                return False, resp
        return True, None

    @app_synchronized
    def doPut(self, req, params):    
        if self.App.ReadOnly:
            return "Read-only instance", 405
        folder = params.get("folder")
        folder = self.App.db().getFolder(folder)
        if folder is None:
            return 404, "Folder not found"
        object = params.get("object")
        if object is None:
            return 400
        ok, resp = self.authenticate(req, folder.Name)
        if not ok:  return resp         # authentication failrure
        tags = params.get("tag", [])
        tv = params.get("tv", None)
        if tv:  tv = float(tv)
        key = params.get("key")
        override = params.get("override", "no") == "yes"
        o = folder.createObject(object)
        if type(tags) == type(""):
            tags = [tags]
        #print "Request body:", req.body
        v = o.createVersion(req.body, tv, tags=tags, key=key, override_key=override)
        return str(v.ID)

    def put(self, req, relpath, **args):
        params = self.parseArgs(relpath, args)
        return self.doPut(req, params)    
        
    def chunked(self, data, chunk_size = 1000000):
        for i in range(0, len(data), chunk_size):
            yield data[i:i+chunk_size]  
        
    @app_synchronized
    def doGet(self, req, params):
        folder = params.get("folder")
        object = params.get("object")
        if folder is None or object is None:
            return 400
        version_id = params.get("id")
        if version_id is not None:  version_id = int(version_id)
        data_key = params.get("data_key")
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
        if data_key is not None:
            data = o.dataByDataKey(data_key)
            if data is None:
                return "Data not found", 404
            else:
                #print("doGet: data:", type(data), repr(data))
                headers = {
                    "Content-Type": "application/octet-stream",
                    "X-UConDB-product-version":  Version,
                    "X-UConDB-data-key":    data_key
                }
                return self.chunked(data), headers
        else:
            if key != None:
                v = o.getVersion(key=key)
            else:
                if tag != None:
                    v = o.getVersion(tag=tag)
                else:
                    v = o.getVersion(tv=tv, tr=tr)
            if v is None:
                return "Version not found", 404
            meta_json = json.dumps(v.metadata)
            if meta_only == "yes":
                ret = (meta_json, "text/json")
            else:
                headers = {
                    "Content-Type": "application/octet-stream",
                    "X-UConDB-product-version":  Version, 
                    "X-UConDB-version-id":  str(v.ID), 
                    "X-UConDB-data-key":    v.DataKey,
                    "X-UConDB-version-key":         v.Key or "null",
                    "X-UConDB-data-size":   str(v.DataSize),
                    "X-UConDB-adler32":     str(v.Adler32),
                    "X-UConDB-tv":          str(v.Tv),
                    "X-UConDB-tr":          str(epoch(v.Tr)),
                    "X-UConDB-metadata":    meta_json,
                    "X-UConDB-metadata-format":    "text/json"
                }
                ret = self.chunked(v.Data), headers
            return ret
            
    def tag(self, req, relpath, tag=None, folder=None, key=None, object=None, version_id=None):
        if self.App.ReadOnly:
            return "Read-only instance", 405
        assert folder is not None
        assert tag is not None
        if version_id is None:
            assert object is not None and key is not None
            o = self.App.db().getFolder(folder).getObject(object)
            v = o.getVersion(key=key)
        else:
            assert object is None and key is None
            version_id = int(version_id)
            folder = self.App.db().getFolder(folder)
            v = folder.getVersionByID(version_id)
        v.addTag(tag)
        return "OK"
            
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
        version_id = int(version_id)
        meta_only = meta_only == "yes"
        folder = self.App.db().getFolder(folder)
        v = folder.getVersionByID(version_id)
        if meta_only:
            meta = {} if v is None else v.metadata
            ret = json.dumps(meta), "text/json"
        else:
            ret = Response(v.Data)
        return ret
        
    def objects(self, req, relpath, folder=None, format="json", **args):
            folder = self.App.db().getFolder(folder)
            objects = folder.listObjects()
            if format == "json":
                out = json.dumps([o.Name for o in objects])
            else:
                # CSV
                out = "Name\n" + "\n".join([o.Name for o in objects])
            return out, "text/"+format      
        
    def tags(self, req, relpath, folder=None, format="json", **args):
            folder = self.App.db().getFolder(folder)
            tags = folder.listTags()
            if format == "json":
                out = json.dumps(tags)
            else:
                # CSV
                out = "Name\n" + "\n".join(tags)
            return out, "text/"+format   

    def create_folder(self, req, relpath, folder=None, owner=None, 
                            read=None, write=None, drop="no", **args):
        if self.App.ReadOnly:
            return "Read-only instance", 405
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
               
    def folders(self, req, relpath, format="json", namespace=None, **args):
        db = self.App.db()
        namespace = namespace or db.DefaultNamespace
        folders = [f.Name for f in db.listFolders(namespace)]
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

        f = self.App.db().getFolder(folder)
        if f is None:
            return 404, f"Folder {folder} not found"
        o = f.getObject(object)
        if o is None:
            return 404, f"Object {object} not found"
        versions = o.listVersions(tr=tr, tv=tv, tr_since=tr_since)
        dicts = (json.dumps(v.as_jsonable()) for v in versions)
        return stream_in_chunks(stream_as_json_list(dicts)), "text/json"
        
    COMPRESS_LIMIT = 10*1024        # do not try to compress short blobs

    def _get_select(self, folder_name, output, object_name=None, tr=None, tag=None, ids=None, tvs=None, keys=None, compress="default"):
        # output can be "meta" or "data"
        #
        # relpath can be either blank, or "folder" or "folder/object"
        # request body is a one of JSON dictionaries:
        # { "ids":[list of version ids] }
        # { "keys":[list of version keys] }, object is required
        # { "tvs":[list of Tvs] }, object is required, tr, tag can be used
        #

        #print("object:", object_name, " keys:", keys)
        
        try:    versions_meta = self._versions_meta(folder_name, object_name=object_name, tvs=tvs, ids=ids, keys=keys, tr=tr, tag=tag)
        except FolderNotFoundError:
            return "Folder not found", 404
        except ObjectNotFoundError:
            return "Object not found", 404

        if output == "meta":
            out = {key:v.as_jsonable() for key, v in versions_meta.items()}
            #print("returning:", out)
            return json.dumps(out), "text/json"
            
        #
        # map data_keys to specs
        #
        data_key_to_specs = {}       # { data_key -> [specs] }, multiple versions may be sharing the same blob
        #print("_get_bulk: versions_meta:", versions_meta)
        for spec, meta in versions_meta.items():
            data_key = meta.DataKey
            data_key_to_specs.setdefault(data_key, []).append(spec)
        
        compression_level = {
            "default":  zlib.Z_DEFAULT_COMPRESSION,
            "fast":  zlib.Z_BEST_SPEED,
            "best":  zlib.Z_BEST_COMPRESSION,
            "no":  "no"
        }[compress]

        folder = self.App.db().getFolder(folder_name)
        blobs = folder.getDataByDataKeys(data_key_to_specs.keys())
        
        def stream_data(blobs):
            def format_blob(specs, blob):
                compressed = False
                orig_size = len(blob)
                if compression_level != "no" and orig_size >= self.COMPRESS_LIMIT:
                    compressed = True
                    orig_size = len(blob)
                    blob = zlib.compress(blob, level=compression_level)
                specs = ",".join([str(spec) for spec in specs])
                flags = ("z" if compressed else "-") + ","      # flags + specs delimiter
                header = ("%s %s %d:" % (flags, specs, len(blob))).encode("utf-8")
                return header + blob
        
            for data_key, blob in blobs:
                specs = data_key_to_specs.get(data_key)
                if specs:
                    yield format_blob(specs, blob)

        return stream_data(blobs), "application/octet-stream; charset=utf-8"
        

    def _lookup_versions(self, folder_name, object_name=None, ids=None, keys=None, key_min=None, key_max=None, tvs=None, tr=None, tag=None):
        #
        # possible ways to find versions:
        #    ids                                
        #    object_name, keys                  
        #    object_name, key_range             
        #    object_name, tvs, tr, tag          
        #    
        # returns generator, yielding pairs:
        #   (id, version)
        #   (key, version)
        #   (tv, version)
        #   
        db = self.App.db()
        folder = db.getFolder(folder_name)
        if folder is None:
            raise FolderNotFoundError(folder_name)
        
        o = None 
        if object_name is not None:
            #print("getting object...")
            o = folder.getObject(object_name)
            #print("got object:", o)
            if o is None:
                raise ObjectNotFoundError(folder_name, object_name)
                
        if tr is not None:  tr = float(tr)
        
        out = None
        
        if ids:
            out = folder.getVersionsByIDs(ids)
        elif keys or key_min or key_max:
            #print("by keys...")
            if o is None:
                raise ParameterError("Object name must be specified")
            out = o.getVersionsByKeys(keys, key_min, key_max)
            #print("   ", out)
        elif tvs:
            # tvs
            if o is None:
                raise ParameterError("Object name must be specified")
            if tr is not None:  tr = float(tr)
            out = o.getVersionsByTvs(tvs, tag=tag, tr=tr)       # returns versions in the same order as tvs
        return out

    def lookup_versions(self, req, relpath, mode="meta", object=None, folder=None, tr=None, tag=None, **args):
        #
        # relpath can be either blank, or "folder" or "folder/object"
        # request body is a one of JSON dictionaries:
        # { "ids":[list of version ids] }
        # { "keys":[list of version keys] } object is required
        # { "key_min":"min key", "key_max":"max key" }, object is required
        # { "tvs":[list of Tvs] }, object is required, tr, tag can be used
        #
        #print("versions_bulk")
        assert req.method.lower() == "post"
        #print("method:", req.method)
        specs = json.loads(req.body)
        
        #print("lookup_versions: specs=", specs)
        
        ids = specs.get("ids")
        keys = specs.get("keys")
        key_min = specs.get("key_min")
        key_max = specs.get("key_max")
        tvs = specs.get("tvs")
        #print("versions_meta_bulk: tvs:", [(type(t), repr(t)) for t in tvs])
        
        #print("keys:", keys)
        words = (relpath or "/").split("/", 1)
        words = (words + [None, None])[:2]
        path_folder, path_object = words
        folder_name = path_folder or folder
        object_name = path_object or object
        
        print("lookup_versions:", folder_name, object_name)
        
        try:    versions = self._lookup_versions(folder_name, object_name, ids=ids, 
                    keys=keys, key_min=key_min, key_max=key_max, tvs=tvs, tr=tr, tag=tag)
        except FolderNotFoundError:
            return f"Folder {folder_name} not found", 404
        except ObjectNotFoundError:
            return f"Object {object_name} not found", 404

        if tvs is not None:
            stream = (v.as_jsonable().update({"lookup_tv":tv}) for tv, v in versions)
        else:
            stream = (v.as_jsonable() for v in versions)
        return stream_as_json_seq(stream), "text/json-seq"
        
    def get_blob(self, req, relpath, folder=None, data_key=None, version_id=None, compress="default"):
        nspecs = sum(int(x) for x in [data_key is not None, version_id is not None])
        if nspec != 1:
            return 400, "One and only one of data_key, version_id must be specified"
        folder_name = folder or relpath
        db = self.App.db()
        folder = db.getFolder(folder_name)
        if folder is None:
            return 404, f"Folder {folder_name} not found"
        if data_key is None:
            version = folder.getVersionByID(int(version_id))
            if version is None:
                return 404, "Version not found"
            blob = version.Data
        else:
            blob = folder.getDataByDataKey(data_key)
            
        if blob is None:
            return 404, "Data not found"
            
        compression_level = {
            "default":  zlib.Z_DEFAULT_COMPRESSION,
            "fast":  zlib.Z_BEST_SPEED,
            "best":  zlib.Z_BEST_COMPRESSION,
            "no":  "no"
        }[compress]

        headers = {"Content-Type":"application/octet-stream"}
        if compression_level != "no" and len(blob) >= self.COMPRESS_LIMIT:
            compressed = True
            blob = zlib.compress(blob, level=compression_level)
            headers["Transfer-Encoding"] = "deflate"
        return blob, headers

    def data_for_versions(self, req, relpath, folder=None, ids=None, filter=None, compress="default"):
        filter=unquote(filter or "") or None
        if ids:
            ids = [int(x) for x in ids.strip.split(",")]
        else:
            assert req.method.lower() == "post"
            params = json.loads(req.body)
            ids = params["ids"]
            filter = params.get("filter")
            
        filter_re = re.compile(filter) if filter else None

        folder_name = folder or relpath
        assert folder_name is not None
        
        t0 = time.time()
        
        try:    versions = self._lookup_versions(folder_name, ids=ids)
        except FolderNotFoundError:
            return "Folder not found", 404
            
        # debug
        versions = list(versions)
        t1 = time.time()
        #print("Versions lookup:", t1-t0)

        #
        # map data_keys to specs
        #
        data_key_to_vids = {}       # { data_key -> [specs] }, multiple versions may be sharing the same blob
        #print("_get_bulk: versions_meta:", versions_meta)
        for v in versions:
            data_key = v.DataKey
            data_key_to_vids.setdefault(data_key, []).append(v.ID)
        
        compression_level = {
            "default":  zlib.Z_DEFAULT_COMPRESSION,
            "fast":  zlib.Z_BEST_SPEED,
            "best":  zlib.Z_BEST_COMPRESSION,
            "no":  "no"
        }[compress]

        folder = self.App.db().getFolder(folder_name)
        if folder is None:
            return 404, "Folder not found"
        t0 = time.time()
        blobs = folder.getDataByDataKeys(data_key_to_vids.keys())
        
        # debug
        #blobs = list(blobs)
        t1 = time.time()
        #print("Data collection:", t1-t0)
        
        def stream_data(blobs):
            
            def transform_blob(filter_re, blob):
                if filter_re is None:
                    return blob
                else:
                    m = filter_re.search(blob)
                    if m is not None:
                        unnamed = m.groups()
                        named = m.groupdict()
                        if not named:
                            return json.dumps(list(unnamed)).encode("utf-8")
                        else:
                            data = named
                            if named:
                                data["__unnamed__"] = list(unnamed)
                            return json.dumps(data).encode("utf-8")
                    else:
                        return None

            def format_blob(specs, blob):
                compressed = False
                orig_size = len(blob)
                if compression_level != "no" and orig_size >= self.COMPRESS_LIMIT:
                    compressed = True
                    orig_size = len(blob)
                    blob = zlib.compress(blob, level=compression_level)
                specs = ",".join([str(spec) for spec in specs])
                flags = ("z" if compressed else "-") + ","      # flags + specs delimiter
                header = ("%s %s %d:" % (flags, specs, len(blob))).encode("utf-8")
                return header + blob
        
            for data_key, blob in blobs:
                blob = transform_blob(filter_re, blob)
                if blob is not None:
                    vids = data_key_to_vids.get(data_key)
                    if vids:
                        print("data_for_versions: yielding:", vids)
                        yield format_blob(vids, blob)
                        print("data_for_versions: yield done")

            print("data_for_versions: stream done")

        return stream_data(blobs), "application/octet-stream; charset=utf-8"
