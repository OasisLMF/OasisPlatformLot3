from copy import deepcopy
from pathlib import Path
# from typing import TypedDict, Dict, NotRequired, Union
from typing import TypedDict, Dict, Optional, Union

from .reader import OasisReader
from ..config import load_class, ConfigError
from ..filestore.backends.local_manager import LocalStorageConnector


class ResolvedReaderEngineConfig(TypedDict):
    path: str
    options: Dict[str, any]


class ResolvedReaderConfig(TypedDict):
    filepath: str
    engine: ResolvedReaderEngineConfig


class InputReaderEngineConfig(TypedDict):
    path: Optional[str]
    options: Optional[Dict[str, any]]


class InputReaderConfig(TypedDict):
    filepath: str
    engine: Optional[Union[str, InputReaderEngineConfig]]


def clean_config(config: Union[str, InputReaderConfig]) -> ResolvedReaderConfig:
    if isinstance(config, (str, Path)) or hasattr(config, "read"):
        config: dict = {
            "filepath": config,
        }
    elif not isinstance(config, dict):
        raise ConfigError(f"df_reader config must be a string or dictionary: {config}")
    else:
        config: dict
        config = deepcopy(config)

    if "filepath" not in config:
        raise ConfigError(f"df_reader config must provide a 'filepath' property: {config}")

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

    cls = load_class(config["engine"]["path"], OasisReader)

    storage = config["engine"]["options"].get("storage", None) or LocalStorageConnector("/")
    return cls(config["filepath"], storage, *args, **kwargs, **config["engine"]["options"])
