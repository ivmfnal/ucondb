from .UCon_backend import UCDataStorageBase
from .UCon_psql import UCDPostgresDataStorage

try:
    from .UCon_couchbase import UCDCouchBaseDataStorage
except:
    pass


if False:           # future development
    try:
        from .UCon_kbs import UCDKBSDataStorage
    except:
        pass
    try:
        from .UCon_blob_server import UCDBlobStorage
    except:
        pass