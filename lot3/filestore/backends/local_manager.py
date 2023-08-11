import contextlib
import io
import logging
import os
import shutil

import fsspec
from pathlib2 import Path
from urllib.parse import urlparse
from urllib.request import urlopen

from lot3.filestore.backends.storage_manager import BaseStorageConnector, MissingInputsException


class LocalStorageConnector(BaseStorageConnector):
    """
    Implements storage for a local filesystem. All paths passed to the
    storage should be relative to the media root.
    """

    storage_connector = 'FS-SHARE'
    fsspec_filesystem_class = fsspec.get_filesystem_class('dir')

    def __init__(self, media_root: str, *args , **kwargs):
        self.media_root = media_root

        super().__init__(*args, **kwargs)

    def can_access(self, path) -> bool:
        """
        Hook to control if a path can be deleted. This can be used to prevent
        deletion from outside the root of the storage
        """
        return os.path.realpath(path).startswith(os.path.realpath(self.media_root) + os.pathsep)

    def filepath(self, reference):
        """ return the absolute filepath 
        """
        fpath = os.path.join(
            self.media_root,
            os.path.basename(reference)
        )
        logging.info('Get shared filepath: {}'.format(reference))
        return os.path.abspath(fpath)

    def get_storage_url(self, filename=None, suffix="tar.gz", **kwargs):
        filename = filename if filename is not None else self._get_unique_filename(suffix)
        return filename, f"file://{Path(self.media_root, filename)}"

    def get_fsspec_storage_options(self):
        return {
            "path": self.media_root
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
