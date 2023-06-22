import importlib

_conf = {}
_engines = {}


class ConfigError(Exception):
    pass


def configure(conf=None):
    global _conf

    _conf = conf or {
        "default": {
            "path": "lot3.df_engine.base.BaseDfEngine",
            "options": {},
        }
    }


def load(file_type):
    found_conf = _conf.get(file_type, _conf.get("default"))
    if not found_conf:
        raise ConfigError(f"'{file_type}' was not fund in the df_engine configuration and no 'default' was provided")

    engine_path = found_conf.get("path")
    if not engine_path:
        raise ConfigError(f"'path' not found in the df_engine config for '{file_type}' files")

    path_split = engine_path.rsplit(".", 1)
    if len(path_split) != 2:
        raise ConfigError(
            f"'path' found in the df_engine config for '{file_type}' files is not valid. "
            "It should be the absolute python path to the class to use eg. lot3.df_engine.base.BaseDfEngine"
        )

    module_path, cls_name = path_split
    module = importlib.import_module(module_path)
    cls = getattr(module, cls_name)

    return cls(**found_conf.get("options", {}))


def get_df_engine(file_type):
    if file_type not in _engines:
        _engines[file_type] = load(file_type)

    return _engines[file_type]
