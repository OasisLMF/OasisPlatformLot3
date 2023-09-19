from tempfile import TemporaryDirectory

from lot3.filestore.backends.local_manager import LocalStorageConnector
from lot3.filestore.config import get_storage_from_config


def test_no_root_dir_is_set___root_dir_is_root():
    storage = LocalStorageConnector()

    assert storage.root_dir == ""


def test_storage_constructed_from_config_matches_initial():
    with TemporaryDirectory() as d:
        storage = LocalStorageConnector(root_dir=d)

        result = get_storage_from_config(storage.to_config())

        assert storage.root_dir == d
        assert isinstance(result, LocalStorageConnector)
        assert result.root_dir == d
