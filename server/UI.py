from webpie import WPHandler, Response
from ucondb import UConDB
import urllib.parse
from datetime import datetime

class UConDBUIHandler(WPHandler):

    def index(self, req, relpath, namespace="", **args):
        # show list of folders
        namespace = namespace or self.App.DefaultNamespace or "public"
        db = self.App.db()
        folders = db.listFolders(namespace)
        for f in folders:
            f.tags = len(f.listTags())
        return self.render_to_response("index.html", namespaces = db.listNamespaces(),
            namespace = namespace, folders = folders)

    PAGE = 100
            
    def paginate(self, nitems, page, url_head):
        if page is None and nitems > self.PAGE:    page = 0
        next_url = prev_url = None
        if page is not None and page > 0:    
            prev_url = url_head + "&page=%d" % (page - 1,)
        if nitems > self.PAGE:
            next_url = url_head + "&page=%d" % (page + 1,)
        return page, prev_url, next_url
        
    def folder(self, req, relpath, begins_with=None, folder=None, page=None, **args):
        db = self.App.db()
        folder = db.getFolder(folder)
        offset = 0
        if page is not None:
            page = int(page)
            offset = self.PAGE*page
            
        objects = folder.listObjects(offset=offset, limit=self.PAGE + 1, begins_with=begins_with)
        url_head = "./objects?folder=%s" % (folder.Name,)
        if begins_with != None:
            url_head += "&begins_with=%s" % (urllib.parse.quote(begins_with),)
        page, prev_page_url, next_page_url = self.paginate(len(objects), page, url_head)
        objects = objects[:self.PAGE]

        return self.render_to_response("folder.html", folder=folder,
            page = page, 
            prev_page_url = prev_page_url, next_page_url = next_page_url,
            objects = objects, begins_with=begins_with or "")

    def object(self, request, relpath, folder=None, name=None, page=None,
                    key=None, tv=None, tr=None, tag=None, **args):
        db = self.App.db()
        limit = 101
        offset = 0
        if page is not None:
            page = int(page)
            offset = self.PAGE*page

        # if the form method was "GET", all these values will be passed as "", not as None's
        tag = tag or None
        if tag:  tag = urllib.parse.unquote_plus(tag)
        tr = tr or None
        if tr:  tr = urllib.parse.unquote_plus(tr)
        tv = tv or None
        if tv:  tv = urllib.parse.unquote_plus(tv)
        key = key or None
        if key:  key = urllib.parse.unquote_plus(key)

        url_head = f"./object?folder={folder}&name={name}"
        folder = db.getFolder(folder)
        obj = folder.getObject(name)


        if key:
            version = obj.getVersion(key=key)
            if version is None:
                return "Not found", 404
            self.redirect(f"./version?vid={version.ID}&folder={folder.Name}")
        else:
            tv_orig = tv
            tr_orig = tr
        
            if tv:
                tv = urllib.parse.unquote_plus(tv)
                try:    tv = float(tv)
                except:
                    tv = datetime.strptime(tv, '%Y-%m-%d %H:%M:%S').timestamp()
            if tr:
                tv = urllib.parse.unquote_plus(tr)
                tr = datetime.strptime(tr, '%Y-%m-%d %H:%M:%S')
            
            versions = list(obj.listVersions(offset=offset, limit=self.PAGE+1, tag=tag, tv=tv, tr=tr))
        
            page, prev_page_url, next_page_url = self.paginate(len(versions), page, url_head)
            versions = versions[:self.PAGE]
        
            return self.render_to_response("object.html", versions = versions, object=obj, folder=folder,
                prev_page_url = prev_page_url, next_page_url = next_page_url, page=page,
                tv=tv_orig, tr=tr_orig, tag=tag
            )

    HEAD = 32*1024
    TAIL = 1024

    def version(self, request, relpath, folder=None, vid=None, **args):
        db = self.App.db()
        folder = db.getFolder(folder)
        vid = int(vid)
        
        version = folder.getVersionByID(vid)
        if version is None:
            return "Not found", 404
        
        data = version.Data
        tail = b''

        try:    
            data = data.decode("utf-8")
            tail = ""
            binary = False
        except: 
            binary = True

        removed = 0
        
        if len(data) > self.HEAD + self.TAIL:
            removed = len(data) - (self.HEAD + self.TAIL)
            tail = data[-self.TAIL:]
            data = data[:self.HEAD]
            
        if binary:
            hex = ["%02X" % (x,) for x in data]
            lines = [hex[i:i+32] for i in range(0, len(data), 32)]
            lines = [" ".join(l) for l in lines]
            data = "\n".join(lines)
            
            hex = ["%02X" % (x,) for x in tail]
            lines = [hex[i:i+32] for i in range(0, len(tail), 32)]
            lines = [" ".join(l) for l in lines]
            tail = "\n".join(lines)
            
        return self.render_to_response("version.html", version=version, folder=folder, data=data, 
            tail=tail, removed=removed)
        

