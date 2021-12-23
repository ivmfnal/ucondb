import requests, json, zlib, random, time
from datetime import datetime

class WebClientError(Exception):
    
    def __init__(self, status_code=None, url=None, error=None):
        self.URL = url
        self.StatusCode = status_code
        self.Error = error
        
    def __str__(self):
        return f"""UConDB Web Client Error:
    URL:              {self.URL}
    HTTP Status Code: {self.StatusCode}
    Message:          {self.Error}
"""
        
def timestamp(t):
    if t is None:
        return None
    if isinstance(t, datetime):
        t = t.timestamp(t)
    return float(t)


class UConDBClient(object):
    
    def __init__(self, server_url, timeout=5):
        """UConDB client constructor

        :param server_url: str - the server endpoint URL, default: value of the UCONDB_SERVER_URL environment variable 
        :param timeout: float - timeout to use communicating with the server, default = 5 seconds
        """

        self.URL = server_url
        self.Timeout = 5
        
    def send_request(self, method, url, **args):
        # implements retrying with exponential retry interval until self.Timeout elapses
        # keeps retrying while status code is 5xx           (limit to 503??)
        retry_interval = 0.1
        t0 = time.time()
        t1 = t0 + self.Timeout
        f = requests.get if method == "get" else requests.post
        response = None
        while time.time() < t1:
            response = f(url, **args)
            if response.status_code//100 != 5:
                break
            else:
                sleep_time = retry_interval * random.random()
                retry_interval *= 1.5
                time.sleep(sleep_time)
        return response

    def get_request(self, url, **args):
        return self.send_request("get", url, **args)

    def post_request(self, url, **args):
        return self.send_request("post", url, **args)

    RS = b"\x1E"     # record separator according to RFC7464
        
    def unpack_json_seq(self, response, chunk_size=8*1024):
        # unpack JSON object stream as described in RFC7464 into a generator of parsed objects
        # assume response_file is a file-like object, which can read() into byte strings
        
        buf = b''
        for chunk in response.iter_content(chunk_size):
            buf += chunk
            parts = buf.split(self.RS)
            if len(parts) > 1:
                parts, buf = parts[:-1], parts[-1]
                for part in parts:
                    part = part.strip()
                    if part:
                        yield json.loads(part)

        buf = buf.strip()
        if buf:
            yield json.loads(buf)

    def unpack_content(self, response):        
        content_type, encoding = self.response_content_type(response)
        assert content_type in ("text/json", "text/json-seq", "text/plain")
        if content_type == "text/json":
            return response.json()
        elif content_type == "text/plain":
            return response.text
        elif content_type == "text/json-seq":
            return self.unpack_json_seq(response)
        else:
            raise WebClientError(error=f"Unsopported content type {content_type}")

    def response_content_type(self, response):
        hdr_value = response.headers.get("Content-Type")
        if hdr_value is None:
            return None, None
        content_type, encoding = None, "utf-8"
        words = hdr_value.split(";")
        content_type = words[0]
        for w in words[1:]:
            w = w.strip()
            if w.startswith("charset="):
                encoding = w.split("=", 1)[1].strip().lower()
        return content_type, encoding
        
    def version(self):
        """
        Returns server version information
        
        :returns: string - version information
        """
        url = self.URL + f"/version"
        response = self.get_request(url)
        return response.text
        
        
    def folders(self):
        """
        Returns list of UConDB folders found in the database
        
        :returns: list of strings - names of the folders found in the database
        """
        url = self.URL + f"/folders"
        response = self.get_request(url)
        if response.status_code != 200:
            raise WebClientError(response.status_code, url, response.text)
        content_type, encoding = self.response_content_type(response)
        assert content_type == "text/json"
        #print("get_versions_meta: response.text:", response.text)
        return self.unpack_content(response)
        
    def objects(self, folder_name):
        """
        Returns list of objects found in the folder
        
        :param folder_name: str - name of the folder
        :returns: list of strings - names as objects found in the folder
        """
        url = self.URL + f"/objects?folder={folder_name}"
        response = self.get_request(url, stream=True)
        if response.status_code != 200:
            raise WebClientError(response.status_code, url, response.text)
        content_type, encoding = self.response_content_type(response)
        assert content_type in "text/json"
        #print("get_versions_meta: response.text:", response.text)
        return self.unpack_content(response)
        
    def lookup_versions(self, folder_name, object_name=None, keys=None, key_min=None, key_max=None,
                    ids=None, tvs=None, tr=None, tag=None, tr_since=None):
        """
        Returns multiple object versions.
        
        :param folder_name: name of the folder (string)
        :param object_name: name of the object (string)
        :param keys: list of version keys (strings). object_name must be specified. If keys present, tvs, tr, tag are ignored
        :param tvs: list of version Tv's (floats). object_name must be specified. tr, tag may be used.
        :param ids: list of version ids (ints). object_name, keys, tvs, tr, tag are ignored.
        :param tr: float - record time. Only versions recorded at or before ``tr`` time will be returned
        :param tr_since: float - record time. Only versions recorded after ``tr_since`` time will be returned
        :param tag: string - only versions with this tag will be returned
        :returns: list of dictionaries with version metadata. If a specified version is not found, it will be absent from the output set. Order is not guaranteed

        Acceptable parameter combinations:
                    
            * object_name, keys   (tvs ignored)
            * object_name, key_range
            * object_name, tvs
            * ids (object_name, keys, tvs ignored)
            * object_name - all versions for the object

        """
        url = self.URL + f"/lookup_versions?folder={folder_name}"
        if object_name is not None:
            url += f"&object={object_name}"
        if ids is not None:
            data = {"ids":ids}
            lookup = "ids"
        elif keys is not None:
            assert object_name is not None
            data = {"keys":keys}
            lookup = "keys"
        elif key_min is not None or key_max is not None:
            assert object_name is not None
            data = {}
            if key_min is not None:
                data["key_min"] = key_min
            if key_max is not None:
                data["key_max"] = key_max
        elif tvs is not None:
            assert object_name is not None
            tr_since = timestamp(tr_since)
            tr = timestamp(tr)
            if tr is not None:
                url += "&tr="+str(tr)
            if tr_since is not None:
                url += "&tr_since="+str(tr_since)
            if tag is not None:
                url += "&tag="+tag
            data = {"tvs":tvs}
            lookup = "tvs"
        else:
            assert object_name is not None
            url = self.URL + f"/versions?object={object_name}&folder={folder_name}"
            #print("requesting", url)
            response = self.get_request(url)
            if response.status_code != 200:
                raise WebClientError(response.status_code, url, response.text)
            content_type, encoding = self.response_content_type(response)
            assert content_type == "text/json"
            #print("get_versions_meta: response.text:", response.text)
            return self.unpack_content(response)
            
        data = json.dumps(data)
        response = requests.post(url, data=data)
        if response.status_code != 200:
            raise WebClientError(response.status_code, url, response.text)
        
        out = self.unpack_content(response)
        return out
        
    def get_data_bulk(self, folder_name, version_ids=None, keys=None):
        """
        Retrieves data BLOBs for multiple object versions
        
        :param folder_name: str - name of the folder
        :param version_ids: list of ints - list of version ids
        :param keys: list of strings - list of version keys
        :yields: genetares sequence of dictionaries with version metadata. Each dictionary also contains "data" with the version data BLOB
        
        ``version_ids`` and ``keys`` can not be specified at the same time
        """
        assert (version_ids is None) != (keys is None)

        url = self.URL + f"/lookup_versions?folder={folder_name}"
        
        params = {"ids":  list(version_ids)} if version_ids is not None else {"keys":  list(keys)}

        response = self.post_request(url, data=json.dumps(params), stream=True)
        if response.status_code != 200:
            raise WebClientError(response.status_code, url, response.text)
            
        versions = response.json()
        data_keys_to_versions = {}
        vids_to_versions = {}
        for v in versions:
            data_key = v["data_key"]
            data_keys_to_versions.setdefault(data_key, []).append(v)
            
        for data_key, versions in data_keys_to_versions.items():
            blob = self.get_data(folder_name, data_key=data_key)
            for version_info in versions:
                version_info["data"] = blob
                yield version_info
                    
    def get_data(self, folder_name, version_id = None, data_key = None):
        """
        Retieves data blob for single object version
        
        :param folder_name: str - name of the folder
        :param version_id: int - version id
        :param data_key: str - version key
        :returns: bytes - version data BLOB
        
        Either version_id or data_key must be specified, but not both
        """
        assert (version_id is None) != (data_key is None)
        url = self.URL + f"/get_blob?folder={folder_name}"
        if version_id is not None:
            url += f"&version_id={version_id}"
        else:
            url += f"&data_key={data_key}"
        response = self.get_request(url)
        if response.status_code != 200:
            raise WebClientError(response.status_code, url, response.text)
        return response.content

    def get_version(self, folder_name, object_name, tv=None, tr=None, tag=None, key=None, id=None, meta_only=True):
        """
        Retieves version metadata, possibly with data BLOB
        
        :param folder_name: str - name of the folder
        :param object_name: str - name of the object
        :param tv: float - validity time
        :param tr: float - record time
        :param tag: str - tag
        :param tag: str - version key
        :param id: int - version id
        :param meta_only: boolean - if True, data BLOB will be also returned
        :returns: dict - version metadata. If ``meta_only`` is False, the dictionary will contain "data" element pointing to the BLOB as bytes
        """
        tvs = [tv] if tv is not None else None
        ids = [id] if id is not None else None
        keys = [key] if key is not None else None
        versions = self.lookup_versions(folder_name, object_name, tvs=tvs, tr=tr, tag=tag, ids=ids, keys=keys)
        if not versions: return None
        versions = list(versions)
        if len(versions) > 1:   raise RuntimeError("More than one version found")
        version_info = versions[0]
        if not meta_only:
            data = self.get_data(folder_name, data_key=version_info["data_key"])
            version_info["data"] = data
        return version_info

if __name__ == "__main__":
    import sys, getopt, pprint
    Usage = """
        python webapi.py  <folder> [options]
            -s <server URL>             # if missing, $UCONDB_SERVER_URL will be used
            -o <object_name>
            -k <key>,...
            -t <tv>,...
            -i <id>,...
            -T <tag>
            -R <tr>
            -m - meta only
    """

    opts, args = getopt.getopt(sys.argv[3:], "t:i:k:T:R:o:m")
    opts = dict(opts)

    server = opts.get("-s") or os.environ.get("UCONDB_SERVER_URL")
    if server is None:
        print("UConDB server URL is not defined")
        print(Usage)
        sys.exit(2)

    server, folder = sys.argv[1:3]
    client = UConDBWebClient(server)


    
    keys = ids = tvs = tag = tr = None
    if "-k" in opts:
        keys = opts["-k"].split(",")
    if "-i" in opts:
        ids = [int(x) for x in opts["-i"].split(",")]
    if "-t" in opts:
        tvs = [float(x) for x in opts["-t"].split(",")]
    tag = opts.get("-T")
    object_name = opts.get("-o")
    if "-R" in opts:
        tr = float(opts["-R"])
    meta_only = "-m" in opts
    
    if meta_only:
        metas = client.get_versions(folder, object_name=object_name, keys=keys, tvs=tvs, ids=ids,
                    tag=tag, tr=tr)
        if keys or ids or tvs:
            for spec, meta in metas.items():
                print(spec,":",pprint.pformat(meta))
        else:
            for m in metas:
                pprint.pprint(m)
    else:
        for spec, blob in client.get_versions_data(folder, object_name=object_name, keys=keys, tvs=tvs, ids=ids,
                tag=tag, tr=tr):
            print(spec, len(blob), repr(blob[:100]))
