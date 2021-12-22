import sys

PY3 = sys.version_info[0] == 3

if PY3:
    def to_bytes(s):
        return s.encode("utf-8") if isinstance(s, str) else bytes(s)
    def to_str(b):
        return b.decode("utf-8", "ignore") if not isinstance(s, str) else b
else:
    def to_bytes(s):
        return bytes(s)
    def to_str(b):
        return str(b)

