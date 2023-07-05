import importlib

from .reader import OasisReader

_conf = {}
_readers = {}


class ConfigError(Exception):
    pass


def reset():
    global _conf, _readers
    _conf = {}
    _readers = {}


def configure(conf=None):
    global _conf

    _conf = conf or {
        "default": {
            "path": "lot3.df_reader.reader.OasisPandasReader",
            "options": {},
        }
    }


def get_df_reader(file_type, path, *args, **kwargs):
    found_conf = _conf.get(file_type, _conf.get("default"))
    if not found_conf:
        raise ConfigError(f"'{file_type}' was not fund in the df_reader configuration and no 'default' was provided")

    engine_path = found_conf.get("path")
    if not engine_path:
        raise ConfigError(f"'path' not found in the df_reader config for '{file_type}' files")

    path_split = engine_path.rsplit(".", 1)
    if len(path_split) != 2:
        raise ConfigError(
            f"'path' found in the df_reader config for '{file_type}' files is not valid. "
            "It should be the absolute python path to the class to use eg. lot3.df_reader.reader.OasisReader"
        )

    module_path, cls_name = path_split
    module = importlib.import_module(module_path)
    cls = getattr(module, cls_name)

    if cls is not OasisReader and OasisReader not in cls.__bases__:
        raise ConfigError(f"'{cls.__name__}' does not extend 'OasisReader'")

    return cls(path, *args, **kwargs, **found_conf.get("options", {}))
