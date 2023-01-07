import sys, yaml
from  configparser import ConfigParser


class   ConfigFile(object):
    def __init__(self, path=None, envVar=None):
        path = path or os.environ.get(envVar)
        self.Config = ConfigParser()
        if path:
            self.Config.read(path)

    def get(self, section, param, default=None):
        try:    return self.Config.get(section, param)
        except: return default

    def __getattr__(self, attr):
        return getattr(self.Config, attr)

cfg_in = sys.argv[1]
cfg = ConfigFile(cfg_in)
out = {}



server_cfg = {
    key: cfg.get("Server", key) for key in "password authentication read_only title".split()
}
server_cfg["read_only"] = server_cfg["read_only"] == "yes"

meta_cfg = {
    key: cfg.get("Database", key) for key in "host password port user namespace name".split()
}
meta_cfg["port"] = int(meta_cfg["port"])
meta_cfg["dbname"] = meta_cfg["name"]
del meta_cfg["name"]

data_cfg = meta_cfg.copy()
data_cfg["type"] = "postgres"

authorization = {}
for folder, value in cfg.items("Authorization"):
    folder_dict = {}
    for word in value.split():
        user, password = word.split(":", 1)
        folder_dict[user] = password
    authorization[folder] = folder_dict
    
config_out = {
    "Server":   server_cfg,
    "Data": data_cfg,
    "Metadata": meta_cfg,
    "Authorization": authorization
}

yaml_out = cfg_in.rsplit(".", 1)[0] + ".yaml"
yaml.dump(config_out, open(yaml_out, "w"))
