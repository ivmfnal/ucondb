import sys
from UConDB import Signature

PY3 = sys.version_info >= (3,)
if PY3:
        from urllib.request import urlopen, Request
else:
        from urllib2 import urlopen, Request


class SingaturePutClient:

    def __init__(self, url, user=None, password=None, server_password=None):
        assert (user is not None and password is not None) \
            or (user is None and server_password is not None)
        self.URL = url
        self.UserPassword = password
        self.User = user
        self.ServerPassword = server_password
        self.HTTPS = url.startswith("https:")
        
    def put(self, folder, name, data, key=None, tv=None):
        url = self.URL + "/data/%s/%s" % (folder, name)
        args = []
        if key is not None:
            args.append("key=%s" % (key,))
        if tv is not None:
            args.append("tv=%s" % (tv,))
        args = "&".join(args)
        if args:
            url += "?" + args

        if self.User is None:
            user = "*"
            password = self.ServerPassword  
        else:
            user = self.User
            password = self.UserPassword

        sig = Signature(folder, data).generate(user, password)
        #print "signature: [%s]" % (sig,)
        request = Request(url, data, {"X-UConDB-Authenticator":sig})
        #print "url:", url
        ssl_context = None
        if self.HTTPS:
            import ssl
            ssl_context = ssl.SSLContext(ssl.PROTOCOL_TLSv1_2)
            ssl_context.verify_mode = ssl.CERT_NONE
        response = urlopen(request, context=ssl_context)
        return response.getcode(), response.read()


if __name__ == '__main__':
    import sys
    
    filename, url, folder, name, key, user, password = sys.argv[1:]
    
    c = SingaturePutClient(url, user=user, password=password)
    status, text = c.put(folder, name, open(filename, "rb").read(), key=key)
    print("status=%s, object id=%s" % (status, text))
