import contextlib
import logging
import os

import fsspec
from pathlib2 import Path

from lot3.filestore.backends.storage_manager import BaseStorageConnector


class LocalStorageConnector(BaseStorageConnector):
    """
    Implements storage for a local filesystem. All paths passed to the
    storage should be relative to the media root.
    """

    storage_connector = 'FS-SHARE'
    fsspec_filesystem_class = fsspec.get_filesystem_class('dir')

    def __init__(self, root_dir: str = '/', **kwargs):
        self.root_dir = root_dir

        super().__init__(**kwargs)

    @property
    def config_options(self):
        return {
            "root_dir": self.root_dir,
        }

    def get_storage_url(self, filename=None, suffix="tar.gz", **kwargs):
        filename = filename if filename is not None else self._get_unique_filename(suffix)
        return filename, f"file://{Path(self.root_dir, filename)}"

    def get_fsspec_storage_options(self):
        return {
            "path": self.root_dir
        }

    @contextlib.contextmanager
    def with_fileno(self, path, mode="rb"):
        yield self.open(path, mode)
