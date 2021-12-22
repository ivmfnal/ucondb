import sys, time, zlib, requests
from datetime import datetime
from requests.auth import HTTPDigestAuth

from dbdig import DbDig
from UCon_backend import UCDataStorageBase
from py3 import to_str, to_bytes

class UCDKBSDataStorage(UCDataStorageBase):

    def __init__(self, url, username, password):
        self.URL = url
        self.Username = username
        self.Password = password

    def putData(self, folder_name, data):
        data = to_bytes(data)
        response = requests.put(self.URL + "/put", data=data, auth=HTTPDigestAuth(self.Username, self.Password))
        if response.status_code//100 == 2:
            return response.text.strip()        # key
        else:
            raise ValueError(f"HTTP status code {response.status_code}")
                   
    def getData(self, folder_name, key):
        response = requests.get(self.URL + f"/get/{key}")
        if response.status_code//100 == 2:
            content_type = response.headers.get("Content-Type")
            blob = response.content
            if content_type == "application/zip":
                blob = zlib.decompress(blob)
            return blob
        else:
            raise ValueError(f"HTTP status code {response.status_code}")
            
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

    def getDataBulk(self, folder_name, keys):
        if not keys: return []
        keys_text = "\n".join(keys)
        response = requests.post(self.URL + f"/get_bulk", data=keys_text, stream=True)
        if response.status_code//100 == 2:
            return response.content
        else:
            raise ValueError(f"HTTP status code {response.status_code}")

        content_type, encoding = self.response_content_type(response)
        assert content_type == "application/octet-stream"
        #
        # Decode the stream of blobs
        #
        eof = False
        while not eof:
            # read header
            h = b""
            c = b""
            while not eof and c != b':':
                c = response.raw.read(1)
                if not c:
                    eof = True
                elif c != b':':
                    h += c

            if not eof:
                #print(f"header: [{h}]")
                h = h.decode(encoding)
                words = h.split()
                flags, specs, size = words[:3]
                delimiter = flags[-1]
                compression = "z" in flags
                size = int(size)
                blob = response.raw.read(size)
                if compression:
                    blob = zlib.decompress(blob)
                keys = specs.split(delimiter)
                for key in keys:
                    yield key, blob
