import psycopg2, sys, time, zlib, hashlib, uuid, string
from datetime import datetime
from .tools import to_str, to_bytes, DbDig, epoch
from base64 import b64encode

#import cStringIO

def cursor_generator(c):
    while True:
        tup = c.fetchone()
        if tup is None:
            break
        yield tup

class UConDBException(Exception):

    def __init__(self, message):
        self.Message = message

    def __str__(self):
        return self.Message
    
class KeyExistsException(UConDBException):
    
    def __init__(self, existing_version):
        UConDBException.__init__(self, "Version of object %s with key %s already exists" % (existing_version.Object.Name, 
            existing_version.Key))
        self.ExistingVersion = existing_version

class UConDB:
    def __init__(self, conn_or_str, data_storage, default_namespace="public"):
        self.Conn = None
        self.ConnStr = None
        if isinstance(conn_or_str, str):
                self.ConnStr = conn_or_str
        else:
                self.Conn = conn_or_str
        self.DataStorage = data_storage
        self.DefaultNamespace = default_namespace
        
    def connect(self):
        if self.Conn == None:
            self.Conn = psycopg2.connect(self.ConnStr)
        return self.Conn
    
    def cursor(self):
        conn = self.connect()
        c = conn.cursor()
        if self.DefaultNamespace:
            c.execute(f"set schema '{self.DefaultNamespace}'")
        return c
        
    def namespace_name(self, name):
        if "." in name:
            return tuple(name.split(".", 1)) + (name,)
        else:
            return self.DefaultNamespace, name, self.DefaultNamespace + "." + name
            
    def createFolder(self, name, owner=None, grants = {}, drop_existing=False):
        namespace, name, fqname = self.namespace_name(name)
        self.DataStorage.createFolder(fqname, owner, grants, drop_existing=drop_existing)
        f = UCDFolder.create(self, fqname, owner, grants, drop_existing)
        return f

    def getFolder(self, name):
        UCDFolder.validate_name(name)
        namespace, name, fqname = self.namespace_name(name)
        if not UCDFolder.exists(self.connect(), fqname):
            return None
        return UCDFolder(self, fqname)

    VColumns = ["id","object","tv","tv_end","tr","deleted","data_key", "key", "adler32", "data_size"]
    VColumns.sort()
    
    TColumns = ["tag_name","version_id"]
    TColumns.sort()
    
    def listNamespaces(self):
        dbd = DbDig(self.connect())
        return sorted(dbd.nspaces())

    def listFolders(self, namespace):
        dbd = DbDig(self.connect())
        tables = dbd.tables(namespace)
        ns = "" if namespace == self.DefaultNamespace else namespace + "."
        #print tables
        tables.sort()
        folders = []
        for t in tables:
            words = t.rsplit("_",1)
            fn = words[0]
            if len(words) == 2 and words[-1] == "_versions":
                if not fn+"_tags" in tables: continue
            v_columns = [c[0] for c in dbd.columns(namespace, t)]
            v_columns.sort()
            #print v_columns
            if v_columns != self.VColumns:  continue
            t_columns = [c[0] for c in dbd.columns(namespace, fn+"_tags")]
            t_columns.sort()
            if t_columns != self.TColumns:  continue
            
            folders.append(ns + fn)
        return [UCDFolder(self, fn) for fn in folders]

    def execute(self, table, sql, args=()):
        #print ("DB.execute(%s, %s, %s)" % (table, sql, args))
        namespace, table_no_ns, fqname = self.namespace_name(table)
        sql = sql.replace('%t', table)
        sql = sql.replace('%T', table_no_ns)
        c = self.cursor()
        #print ("executing: <%s>, %s" % (sql, args))
        t0 = time.time()
        c.execute(sql, args)
        #print "executed. t=%s" % (time.time() - t0,)
        return c

    def disconnect(self):
        if self.Conn:   self.Conn.close()
        self.Conn = None


class UCDFolder:

    CreateTables = """
create table %t_versions
(   
    id          serial primary key,
    key         text    null,
    object      text,
    tv          float,
    tv_end      float default null,
    tr          timestamp with time zone default current_timestamp,
    deleted     boolean default 'false',
    data_size   bigint,
    data_key    text,
    adler32     bigint
);

create index %T_versions_inx on %t_versions (object, tv, tr);

create unique index %T_object_key_inx on %t_versions (object, key);

create table %t_tags
(
    version_id int  references %t_versions(id) on delete cascade,
    tag_name    text,
    primary key (version_id, tag_name)
);

create table %t_salt
(
    salt        text    primary key,
    time_used   timestamp with time zone default current_timestamp
);
"""


    DropTables = """
        drop table %t_tags;
        drop table %t_versions;
        drop table %t_salt;
    """

    NameSafe = string.ascii_letters + string.digits + '._'

    def __init__(self, db, name):
        self.validate_name(name)
        self.Name = name
        self.DB = db
        self.DataInterface = self.DB.DataStorage
        
    def __str__(self):
        return "UCDFolder(%s)" % (self.Name,)
        
    @staticmethod
    def validate_name(name):
        valid = name and \
            all(x in UCDFolder.NameSafe for x in name) \
            and sum(x == '.' for x in name) <= 1 \
            and not (name.startswith('.') or name.endswith('.'))
        if not valid:
            raise ValueError("syntax error in forlder name" % (name,))

    @staticmethod
    def exists(db, fqname):
        dbd = DbDig(db)
        namespace, name = fqname.split(".", 1)
        tables = dbd.tables(namespace)
        return f"{name}_versions" in tables

    def dataInterface(self):
        return self.DataInterface

    def execute(self, sql, args=()):
        #print("Folder.execute: Name:", self.Name, "sql, args:", (sql, args))
        return self.DB.execute(self.Name, sql, args)

    @staticmethod
    def create(db, name, owner, grants = {}, drop_existing=False):
        t = UCDFolder(db, name)
        t.createTables(owner, grants, drop_existing)
        return t

    def checkSalt(self, salt):
        exists = False
        try:    
            c = self.execute("""select salt, time_used from %t_salt
                    where salt = %s""", (salt,))
            if c.fetchone():
                return False
        except:
            # in case the table does not exist yet
            c = self.execute("""rollback; create table %t_salt
                            (
                                salt        text    primary key,
                                time_used   timestamp with time zone default current_timestamp
                            )""")
        self.execute("""insert into %t_salt(salt) values (%s); commit""", (salt,))
        return True
            
    def tableNames(self):
        #return [self.Name + "_" + s for s in ("snapshot", "tag", "tag_snapshot", 
        #            "snapshot_data", "update")]

        return [self.Name + "_" + s for s in ("versions", "tags", "salt")]

    def createTables(self, owner = None, grants = {}, drop_existing=False):
        exists = True
        c = self.DB.cursor()
        try:    
            c.execute("""select * from %s_versions limit 1""" % (self.Name,))
        except: 
            c.execute("rollback")
            exists = False
        if exists and drop_existing:
            self.execute(self.DropTables)
            exists = False

        if not exists:
            c = self.DB.cursor()
            if owner:
                c.execute("set role %s" % (owner,))
            sql = self.CreateTables.replace('%t', self.Name)
            self.execute(sql)
            read_roles = ','.join(grants.get('r',[]))
            if read_roles:
                grant_sql = """grant select on %t_versions, %t_tags, %t_versions_id_seq to """ + read_roles         # + %t_snapshot_data,
                #print grant_sql
                self.execute(grant_sql)
            write_roles = ','.join(grants.get('w',[]))
            if write_roles:
                grant_sql = """grant insert, delete, update on %%t_versions, %%t_tags to %(roles)s; 
                    grant all on %%t_versions_id_seq to %(roles)s;""" % {'roles':write_roles}     # +%%t_snapshot_data,
                # grant_sql
                self.execute(grant_sql)
            c.execute("commit")

    def fetchData(self, data_key):
        return self.DataInterface.fetchData(self.Name, data_key)
                        
    #
    # Public API
    #                        
    def createObject(self, name):
        if not name:
            raise ValueError("Object name is empty or None")
        return UCDObject(self, name)
        
    def getObject(self, name):
        c = self.execute("""
            select * from %t_versions
                where object=%s and not deleted
                limit 1""", (name,))
        tup = c.fetchone()
        if not tup: return None
        return UCDObject(self, name)
        
    def objectCount(self):
        c = self.execute("""
            select count(*) from (
                select distinct object from %t_versions
                    where not deleted 
                    order by object) as objects""")
        return c.fetchone()[0]
                              
    def versionCount(self):
        c = self.execute("""
            select count(*) from %t_versions
                    where not deleted""")
        return c.fetchone()[0]
                              
    def listObjects(self, limit=None, offset=None, begins_with=None):
        assert limit is None or isinstance(limit, int)
        assert offset is None or isinstance(offset, int)
        assert begins_with is None or isinstance(begins_with, str)
        page = ""
        if limit != None:
            page = " limit %d " % (limit,)
        if offset != None:
            page += " offset %d " % (offset,)
        sql = f"""select distinct object from %t_versions
                where not deleted 
                    and ( %s is null or object like (%s || '%%') )
                order by object 
                {page}
                """
        #print sql
        c = self.execute(sql, (begins_with, begins_with))
        return [UCDObject(self, name) for (name,) in c.fetchall()]   
        
    def listTags(self):
        #print self.Name
        c = self.execute("""
            select tag_name from %t_tags order by tag_name""")
        return [x[0] for x in c.fetchall()]  
        
    def getVersionByID(self, vid):
        c = self.execute("""select object, tr, tv, data_key, data_size, key, adler32 from %t_versions where id=%s""", (vid,))
        tup = c.fetchone()
        if tup == None: return None
        name, tr, tv, data_key, data_size, key, adler32 = tup
        o = UCDObject(self, name)
        v = UCDVersion(o, vid, tr, tv, data_key, data_size, adler32, key=key)
        return v
        
    def getVersionsByIDs(self, ids):
        # yields pairs (id, UCVVersion)
        c = self.execute("""select object, id, tr, tv, data_key, data_size, key, adler32 from %t_versions 
            where id=any(%s)""", (ids,))
        for name, vid, tr, tv, data_key, data_size, key, adler32 in cursor_generator(c):
            o = UCDObject(self, name)
            v = UCDVersion(o, vid, tr, tv, data_key, data_size, adler32, key=key)
            yield v
            
    def getVersionsDataByIDs(self, version_ids):
        # generator of pairs (version_id, BLOB). If a version is not found, it will not be present in the output
        data_key_to_id = {v.DataKey:v.ID for v in self.getVersionsByIDs(version_ids)}
        
        for data_key, blob in self.DataInterface.getDataBulk(self.Name, list(data_key_to_id.keys())):
            yield data_key_to_id[data_key], blob

    def getDataByDataKeys(self, data_keys):
        # generator of pairs (version_id, BLOB). If a version is not found, it will not be present in the output

        for data_key, blob in self.DataInterface.getDataBulk(self.Name, data_keys):
            yield data_key, blob

    def getDataByDataKey(self, data_key):
        return self.dataInterface().getData(self.Name, data_key)

class UCDObject:

    def __init__(self, folder, name):
        self.Folder = folder
        self.Name = name
        self._LastVersion = None   

    def dataInterface(self):
        return self.Folder.dataInterface()

    def execute(self, sql, args=()):
        #print "Table.execute(%s, %s)" % (sql, args)
        return self.Folder.execute(sql, args)
        
    def dataByDataKey(self, data_key):
        return self.dataInterface().getData(self.Folder.Name, data_key)        

    def createVersion(self, data, tv=None, key=None, tags=[], override_key=False):
        data = to_bytes(data)
    
        #print("createVersion: len(data)=", len(data))

        tv = tv or 0.0
    
        c = self.execute("begin")
        if key != None:
            ov = self.getVersion(key=key)
            if ov != None:
                if not override_key:
                    c.execute("rollback")
                    raise KeyExistsException(ov)
                else:
                    c = self.execute("""update %t_versions
                            set key=null
                            where key=%s and object=%s""", (key, self.Name))

        data_size = len(data)
        data_key = self.dataInterface().putData(self.Folder.Name, data)

        a32 = zlib.adler32(data) & 0xFFFFFFFF
        c = self.execute("""insert into %t_versions(id, key, tv, tr, object, data_key, data_size, adler32)
                    values(default, %s, %s, default, %s, %s, %s, %s)
                    returning id, tr, tv""", (key, tv, self.Name, data_key, data_size, a32))
        vid, tr, tv = c.fetchone()
        v = UCDVersion(self, vid, tr, tv, data_key, data_size, a32, key=key)
        for t in tags:
            v.addTag(t)
        c.execute("commit")
        return v
        
    def listVersions(self, tr=None, tv=None, tr_since=None, tag=None, limit=None, offset=None):
        assert tv is None or isinstance(tv, (int, float))
        assert tr_since is None or isinstance(tr_since, datetime)
        assert tag is None or isinstance(tag, str)
        assert limit is None or isinstance(limit, int)
        assert offset is None or isinstance(offset, int)
        filters = ""
        if tr_since is not None:    filters += " and v.tr > '%s' " % (tr_since,)
        if tv is not None:    filters += " and v.tv <= %s " % (tv,)
        if tr is not None:    filters += " and v.tr <= '%s' " % (tr,)
        limit = f"limit {limit}" if limit is not None else ""
        offset = f"offset {offset}" if offset is not None else ""
        if tag is None:
            sql = f"""select id, key, tr, tv, data_key, data_size, adler32
                    from %t_versions v
                    where not deleted and object=%s 
                    {filters}
                    order by tr desc, tv
                    {limit} {offset} 
                    """
            c = self.execute(sql, (self.Name,))
        else:
            sql = f"""select v.id, v.key, v.tr, v.tv, v.data_key, v.data_size, v.adler32
                    from %t_versions v, %t_tags t
                    where not v.deleted and v.object=%s 
                        and v.id = t.version_id 
                        and t.tag_name = %s
                    {filters}
                    order by v.tr desc, v.tv
                    {limit} {offset} 
                    """
            c = self.execute(sql, (self.Name, tag))
            
        #print("listVersions: sql:", sql)
        return (UCDVersion(self, vid, tr, tv, data_key, data_size, adler32, key=key) for vid, key, tr, tv, data_key, data_size, adler32 in c.fetchall())
        
    def getVersionsForInterval(self, tv0, tv1, tag=None, tr=None):
        # returns list of versions sorted by Tv in ascending order
        assert isinstance(tv0, (int, float))
        assert isinstance(tv1, (int, float))
        assert tr is None or isinstance(tr, datetime)
        assert tag is None or isinstance(tag, str)
        
        v0 = self.getVersion(tag=tag, tr=tr, tv=tv0) # version immediately before tv0

        tr_filter = "" if tr is None else " and v.tr <= '%s' " % (tr,)

        if tag is None:
            sql = f"""select id, key, tr, tv, data_key, data_size, adler32
                    from %t_versions v
                    where 
                        not deleted 
                        and object=%s 
                        and tv > %s and tv <= %s
                        and (%s is null or v.tr <= %s)
                    order by tr desc, tv desc
                    """
            c = self.execute(sql, (self.Name, tv0, tv1, tr, tr))
        else:
            sql = f"""select v.id, v.key, v.tr, v.tv, v.data_key, v.data_size, v.adler32
                    from %t_versions v, %t_tags t
                    where not v.deleted and v.object=%s 
                        and v.id = t.version_id 
                        and t.tag_name = %s
                        and tv > %s and tv <= %s
                        and (%s is null or v.tr <= %s)
                    order by v.tr desc, v.tv desc
                    """
            c = self.execute(sql, (self.Name, tag, tv0, tv1, tr, tr))
            
        last_version = None
        versions = []
        for vid, key, tr, tv, data_key, data_size, adler32 in cursor_generator(c):
            if last_version is None or tv < last_version.Tv:
                v = UCDVersion(self, vid, tr, tv, data_key, data_size, adler32, key=key)
                versions.insert(0, v)
                last_version = v
        if v0 is not None:
            versions.insert(0, v0)
        return versions
        
    def getVersionsByTvs(self, tvs, tag=None, tr=None):
        #
        # yields pairs (tv, UCDVersion)
        #
        
        # sort tv list but remember original order so that the output can be matched back to the input
        tv_indexed = sorted([(t,i) for i, t in enumerate(tvs)])
        original_inx = [i for t,i in tv_indexed]
        tvs = [t for t,i in tv_indexed]
        #print("getVersionsByTvs: tvs:", tvs)
        tv0 = tvs[0]
        tv1 = tvs[-1]
        versions = self.getVersionsForInterval(tv0, tv1, tag=tag, tr=tr)        
        # assume versions is sorted by tv and there are no repeating tvs
        #print("all_versions:", versions)
        if versions:
            #
            # skip to first tv, for which there is a version
            #
            v0 = versions[0]
            for it, t in enumerate(tvs):
                if t >= v0.Tv:
                    break
        
            if it < len(tvs):
                #
                # at this point, tvs[it] >= versions[0].Tv
                #
                prev_v = versions[0]
                for v in versions[1:]:
                    while it < len(tvs) and tvs[it] < v.Tv:
                        yield tvs[it], prev_v
                        it += 1
                    prev_v = v

                while it < len(tvs):
                    yield tvs[it], prev_v
                    it += 1

    def getVersion(self, tag=None, tr=None, tv=None, key=None):
        # if tag is specified, tr is ignored

        if tv is None:  tv = time.time()

        if key is not None:
            # get by key
            c = self.execute("""select v.id, v.tr, v.tv, v.data_key, v.data_size, v.adler32
                from %t_versions v
                where v.object = %s and v.key = %s""", (self.Name, key))
            tup = c.fetchone()
            if not tup: return None
            vid, tr, tv, data_key, data_size, adler32 = tup
            return UCDVersion(self, vid, tr, tv, data_key, data_size, adler32, key=key)

        if type(tr) in (type(1), type(1.0)):
            tr = datetime.fromtimestamp(tr)
            
        if tag is not None:
            c = self.execute("""select v.id, v.tr, v.tv, v.data_key, v.data_size, v.key, v.adler32
                from %t_versions v, %t_tags t
                where v.object = %s and v.tv <= %s and
                    v.id = t.version_id and
                    t.tag_name = %s
                order by v.tr desc, v.tv desc
                limit 1""", (self.Name, tv, tag))
        elif tr is not None:
            if type(tr) in (type(1), type(1.0)):
                tr = datetime.fromtimestamp(tr)
            
            c = self.execute("""select v.id, v.tr, v.tv, v.data_key, data_size, v.key, v.adler32
                from %t_versions v
                where v.object = %s and v.tr < %s and v.tv <= %s
                order by v.tr desc, v.tv desc
                limit 1""", (self.Name, tr, tv))
        else:
            c = self.execute("""select v.id, v.tr, v.tv, v.data_key, data_size, v.key, v.adler32
                from %t_versions v
                where v.object = %s and v.tv <= %s
                order by v.tr desc, v.tv desc
                limit 1""", (self.Name, tv))
            
        tup = c.fetchone()
        if not tup: return None
        vid, tr, tv, data_key, data_size, key, adler32 = tup
        return UCDVersion(self, vid, tr, tv, data_key, data_size, adler32, key=key)

    def getVersionsByKeys(self, keys=None, key_min=None, key_max=None):
        # key_range: (min, max)
        #   min and max can be None
        #   if key_range is given, returns all versions with x <= key < y
        # yields pairs (key, UCDVersion)
        #print("getVersionsByKeys: keys:", keys)
        
        t0 = time.time()
        
        if keys:
            c = self.execute(f"""select v.id, v.tr, v.tv, v.data_key, data_size, v.key, v.adler32
                from %t_versions v
                where v.object = %s and v.key = any(%s)
                """, (self.Name, keys))
        else:
            c = self.execute(f"""select v.id, v.tr, v.tv, v.data_key, data_size, v.key, v.adler32
                from %t_versions v
                where v.object = %s
                    and ( %s is null or key >= %s )
                    and ( %s is null or key < %s )
                """, 
                (self.Name, key_min, key_min, key_max, key_max))
                
        t1 = time.time()

        for vid, tr, tv, data_key, data_size, key, adler32 in cursor_generator(c):
            yield UCDVersion(self, vid, tr, tv, data_key, data_size, adler32, key=key)
            
        t2 = time.time()
        
        #print("getVersionsByKeys times:", t1-t0, t2-t1)

    @property
    def LastVersion(self):
        if self._LastVersion == None:
            self._LastVersion = self.getVersion()
        return self._LastVersion
        
    def versionCount(self, include_deleted = False):
        not_deleted = "and not deleted" if not include_deleted else ""
        c = self.execute(f"""select count(*)
            from %t_versions v
            where v.object = %s {not_deleted}""", (self.Name,))
        return c.fetchone()[0]
        
class UCDVersion:

    def __init__(self, object, vid, tr, tv, data_key, data_size, adler32, key=None):
        self.Object = object
        self.ID = vid
        self.Tr = tr
        self.Tv = tv
        self.DataKey = data_key
        self.DataSize = data_size
        self.Adler32 = adler32
        self.Key = key
        self.LookupTv = None        # if the version was found by Tv, this will be populated
        self.__Data = None
        self.Tags = None
        
    def execute(self, sql, args=()):
        #print "Table.execute(%s, %s)" % (sql, args)
        return self.Object.execute(sql, args)

    def __get_data(self):
        if self.__Data == None:   
            self.__Data = self.Object.dataInterface().getData(self.Object.Folder.Name, self.DataKey)
        return self.__Data

    def __set_data(self, data):
        self.__Data = data

    Data = property(__get_data, __set_data)
     
    def addTag(self, tag):
        c = self.execute("""
            delete from %t_tags
                where tag_name = %s and version_id = %s;
            insert into %t_tags(version_id, tag_name)
                values(%s, %s);
            commit""", (tag, self.ID, self.ID, tag))
        self.Tags = (self.Tags or []).append(tag)
            
    def getTags(self):
        if self.Tags is None:
            c = self.execute("""
                select tag_name from %t_tags
                    where version_id=%s order by tag_name""", (self.ID,))
            self.Tags = [x[0] for x in c.fetchall()]
        return self.Tags
        
    @property
    def metadata(self):
        out = {
            "object":   self.Object.Name,
            "folder":   self.Object.Folder.Name,
            "tv":   self.Tv,
            "tr":   epoch(self.Tr),
            "tags": self.getTags(),
            "adler32":  self.Adler32,
            "uadler32": self.Adler32 & 0xFFFFFFFF,
            "data_size": self.DataSize,
            "data_key": self.DataKey,
            "id":   self.ID,
            "key":  self.Key
        }
        if self.LookupTv is not None:
            out["lookup_tv"] = self.LookupTv
        return out
        
    def set_lookup_tv(self, tv):
        self.LookupTv = tv
        return self

    def as_jsonable(self, with_data=False, data_format="base64", add={}):
        data = self.metadata
        if with_data:
            data["data"] = b64encode(self.Data).decode("utf-8")
            data["data_format"] = "base64"
        if add:
            data.update(add)
        return data


            
                                
                    
if __name__ == '__main__':

    import sys, getopt, os
    from UCon_psql import UCDataStorageBase

    Usage = """
        UConDB.py <DB connect string> <command> <args>
        Commands:
            get <object name>
            put <object name> <data>
    """
    
    if len(sys.argv) < 4:
        print(Usage)
        sys.exit(0)
    
    connstr = sys.argv[1]
    ds = UCDPostgresDataStorage(connstr)
    db = UConDB(connstr, ds)
    
    f = db.createFolder("test", "ivm", {}, False)
    
    cmd = sys.argv[2]
    object = sys.argv[3]
    
    if cmd == "get":
        o = f.getObject(object)
        v = o.getVersion()
    elif cmd == "put":
        o = f.createObject(object)
        v = o.createVersion(sys.argv[4])
    print("Tr:      ", v.Tr)
    print("Tv:      ", v.Tv)
    print("Data key:", v.DataKey)
    print("Data:    ", v.Data)
        
    
