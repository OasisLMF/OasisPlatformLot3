import importlib
from copy import deepcopy
from pathlib import Path
from typing import TypedDict, Dict, NotRequired, Union

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

    _conf = conf or _conf or {
        "default": {
            "path": "lot3.df_reader.reader.OasisPandasReader",
            "options": {},
        }
    }


class ResolvedReaderEngineConfig(TypedDict):
    path: str
    options: Dict[str, any]


class ResolvedReaderConfig(TypedDict):
    path: str
    engine: ResolvedReaderEngineConfig


class InputReaderEngineConfig(TypedDict):
    path: NotRequired[str]
    options: NotRequired[Dict[str, any]]


class InputReaderConfig(TypedDict):
    path: str
    engine: NotRequired[Union[str, InputReaderEngineConfig]]


def clean_config(config: Union[str, InputReaderConfig]) -> ResolvedReaderConfig:
    if isinstance(config, (str, Path)) or hasattr(config, "read"):
        config: dict = {
            "path": config,
        }
    elif not isinstance(config, dict):
        raise ConfigError(f"df_reader config must be a string or dictionary: {config}")
    else:
        config: dict
        config = deepcopy(config)

    if "path" not in config:
        raise ConfigError(f"df_reader config must provide a 'path' property: {config}")

    if "engine" not in config:
        config["engine"] = {
            "path": "lot3.df_reader.reader.OasisPandasReader",
            "options": {},
        }
    elif isinstance(config.get("engine"), str):
        config["engine"] = {
            "path": config.get("engine"),
            "options": {}
        }
    else:
        config["engine"].setdefault("path", "lot3.df_reader.reader.OasisPandasReader")
        config["engine"].setdefault("options", {})

    return config


def get_df_reader(config, *args, **kwargs):
    config = clean_config(config)

    path_split = config["engine"]["path"].rsplit(".", 1)
    if len(path_split) != 2:
        raise ConfigError(f"'path' found in the df_reader config is not valid: {config['engine']['path']}")

    module_path, cls_name = path_split
    module = importlib.import_module(module_path)
    cls = getattr(module, cls_name)

    if cls is not OasisReader and OasisReader not in cls.__bases__:
        raise ConfigError(f"'{cls.__name__}' does not extend 'OasisReader'")

    return cls(config["path"], *args, **kwargs, **config["engine"]["options"])
