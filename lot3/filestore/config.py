import json
import os
from typing import Optional, TypedDict, Union

from lot3.config import ConfigError, load_class
from lot3.filestore.backends.local_manager import LocalStorageConnector
from lot3.filestore.backends.storage_manager import BaseStorageConnector


class LocalStorageConfig(TypedDict):
    root_dir: str


class StorageConfig(TypedDict):
    storage_class: str
    options: Union[LocalStorageConfig]


def get_storage_from_config(config: StorageConfig):
    cls = load_class(config["storage_class"], BaseStorageConnector)
    return cls(**config["options"])


def get_storage_from_config_path(config_path: Optional[str], fallback_path: str):
    """
    Loads the config from the supplied path. If no config path is provided or the path
    doesn't exist a local file store object will be created with the root set to the
    fallback path.

    :param config_path: The path to the config file to load
    :param fallback_path: The path for the local file store should the config path not exist
    """
    if config_path and os.path.exists(config_path):
        with open(config_path) as f:
            config: StorageConfig = json.load(f)
            model_storage = get_storage_from_config(config)
    elif fallback_path:
        model_storage = LocalStorageConnector(
            root_dir=fallback_path,
        )
    else:
        raise ConfigError(
            "The given config path does not exist and no fallback path was given to create the local storage from"
        )

    return model_storage
