import config_default, config_override


class Dict(dict):
    """
    Simple dict but support access as x.y style.
    """
    def __init__(self, names=(), values=(), **kw):
        super(Dict, self).__init__(**kw)
        for k, v in zip(names, values):
            self[k] = v

    def __getattr__(self, key):
        try:
            return self[key]
        except KeyError:
            raise AttributeError(r"'Dict' object has no attribute '%s'" % key)

    def __setattr__(self, key, value):
        self[key] = value

"""
def merge(defaults, override):
    r = {}
    for k, v in defaults.items():
        if k in override:
            if isinstance(v, dict):
                r[k] = merge(v, override[k])
            else:
                r[k] = override[k]
        else:
            r[k] = v
    return r
"""


def merge2(defaults, override):
    r = defaults.copy()
    for k, v in r.items():
        if k in override:
            if isinstance(v, dict):
                r[k] = merge2(v, override[k])
            else:
                r[k] = override[k]
    return r


def toDict(d):
    D = Dict()
    for k, v in d.items():
        if isinstance(v, dict):
            D[k] = toDict(v)        # This is cursive case
        else:
            D[k] = v                # This is base case
    return D

try:
    configs = merge2(config_default.configs, config_override.configs)
    configs = toDict(configs)
except ImportError:
    pass

