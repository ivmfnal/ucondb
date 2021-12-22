import sys

PY3 = sys.version_info >= (3,)
if PY3:
        from urllib.request import urlopen, Request, HTTPDigestAuthHandler, build_opener
else:
        from urllib2 import urlopen, Request, HTTPDigestAuthHandler, build_opener


URLHead = "http://rexdb02.fnal.gov:8088/ucondb/app"
user = "ivm"
password = "12345"

N = 10
data = "@"*10000        # 10K data

class PasswordManager:

    def __init__(self, username, password):
        self.Username = username
        self.Password = password

    def find_user_password(self, realm, uri):
        return (self.Username, self.Password)

    def add_password(self, realm, uri, user, pwd):
        pass

auth_handler = HTTPDigestAuthHandler(PasswordManager(user, password))
opener = build_opener(auth_handler)
for _ in range(N):
    response = opener.open(URLHead + "/data/test/test_file", data)
    print(response.getcode(), response.read())
