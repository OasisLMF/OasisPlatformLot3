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
    supports_bin_files = True

    def __init__(self, root_dir: str = '/', **kwargs):
        self.root_dir = root_dir

        super().__init__(**kwargs)

    @property
    def config_options(self):
        return {
            "root_dir": self.root_dir,
        }

    def can_access(self, path) -> bool:
        """
        Hook to control if a path can be deleted. This can be used to prevent
        deletion from outside the root of the storage
        """
        return os.path.realpath(path).startswith(os.path.realpath(self.root_dir) + os.pathsep)

    def filepath(self, reference):
        """ return the absolute filepath 
        """
        fpath = os.path.join(
            self.root_dir,
            os.path.basename(reference)
        )
        logging.info('Get shared filepath: {}'.format(reference))
        return os.path.abspath(fpath)

    def get_storage_url(self, filename=None, suffix="tar.gz", **kwargs):
        filename = filename if filename is not None else self._get_unique_filename(suffix)
        return filename, f"file://{Path(self.root_dir, filename)}"

    def get_fsspec_storage_options(self):
        return {
            "path": self.root_dir
        }

    @property
    def fs(self) -> fsspec.AbstractFileSystem:
        if not self._fs:
            self._fs = self.fsspec_filesystem_class(
                **self.get_fsspec_storage_options()
            )
        return self._fs

    def exists(self, path):
        return self.fs.exists(path)

    def isfile(self, path):
        return self.fs.isfile(path)

    @contextlib.contextmanager
    def open(self, path, *args, **kwargs):
        with self.fs.open(path, *args, **kwargs) as f:
            yield f
