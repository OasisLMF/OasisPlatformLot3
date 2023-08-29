import contextlib
import os
import string
import uuid
from tempfile import TemporaryDirectory

import pytest
from hypothesis import given, settings
from hypothesis.strategies import text

from lot3.filestore.backends.aws_storage import AwsObjectStore
from lot3.filestore.backends.local_manager import LocalStorageConnector

test_file_name = "test_file.txt"
test_dir_name = "test_dir"


@contextlib.contextmanager
def aws_storage(**kwargs):
    kwargs.setdefault("bucket_name", uuid.uuid4().hex)
    kwargs.setdefault("access_key", "LSIAQAAAAAAVNCBMPNSG")
    kwargs.setdefault("secret_key", "ANYTHING")
    kwargs.setdefault("endpoint_url", "http://localhost:4566")
    kwargs.setdefault("cache_dir", None)

    fs = AwsObjectStore(**kwargs)
    fs.fs.mkdirs("")

    yield fs


@contextlib.contextmanager
def local_storage(root_dir="", **kwargs):
    kwargs.setdefault("cache_dir", None)
    with TemporaryDirectory() as root:
        yield LocalStorageConnector(root_dir=os.path.join(root, root_dir), **kwargs)


storage_factories = [
    # local_storage,
    aws_storage,
]


#
# get
#

@pytest.skip()
@pytest.mark.parametrize("storage_factory", storage_factories)
@given(content=text(alphabet=string.digits + string.ascii_letters))
@settings(deadline=None)
def test_get_copies_a_file_to_the_correct_location(storage_factory, content):
    with storage_factory() as storage, TemporaryDirectory() as dst, TemporaryDirectory() as data:
        with open(os.path.join(data, test_file_name), "w") as f:
            f.write(content)

        storage.put(os.path.join(data, test_file_name), test_file_name)
        storage.get(test_file_name, dst)

        with open(os.path.join(dst, test_file_name)) as f:
            assert f.read() == content


@pytest.skip()
@pytest.mark.parametrize("storage_factory", storage_factories)
@given(content=text(alphabet=string.digits + string.ascii_letters))
@settings(deadline=None)
def test_cannot_copy_outside_of_root(storage_factory, content):
    with storage_factory(root_dir="root") as storage, TemporaryDirectory() as dst, TemporaryDirectory() as data:
        other_dir = os.path.join(storage.fs.path, "..", "other")

        with open(os.path.join(data, test_file_name), "w") as f:
            f.write(content)

        storage.fs.fs.mkdirs(other_dir)
        storage.fs.fs.put(os.path.join(data, test_file_name), other_dir)

        with pytest.raises(FileNotFoundError):
            storage.get(os.path.join("..", "other", test_file_name), dst)


#
# put
#

@pytest.skip()
@pytest.mark.parametrize("storage_factory", storage_factories)
@given(content=text(alphabet=string.digits + string.ascii_letters))
@settings(deadline=None)
def test_put_copies_a_file_to_the_correct_location(storage_factory, content):
    with storage_factory(root_dir="root") as storage, TemporaryDirectory() as src:
        with open(os.path.join(src, test_file_name), "w") as f:
            f.write(content)

        storage.put(os.path.join(src, test_file_name), filename=test_file_name)

        with storage.fs.fs.open(os.path.join(storage.fs.path, test_file_name), "r") as f:
            assert f.read() == content


@pytest.skip()
@pytest.mark.parametrize("storage_factory", storage_factories)
@given(content=text(alphabet=string.digits + string.ascii_letters))
@settings(deadline=None)
def test_put_copies_directory_to_a_tar(storage_factory, content):
    with storage_factory() as storage, TemporaryDirectory() as src, TemporaryDirectory() as extract_dir:
        with open(os.path.join(src, test_file_name), "w") as f:
            f.write(content)

        storage.put(src, filename=test_dir_name)

        storage.extract(test_dir_name, extract_dir)
        with open(os.path.join(extract_dir, test_file_name)) as f:
            assert f.read() == content


#
# exists
#

@pytest.skip()
@pytest.mark.parametrize("storage_factory", storage_factories)
def test_path_inside_root_doesnt_exist___exists_is_false(storage_factory):
    with storage_factory() as storage:
        assert not storage.exists(test_file_name)


@pytest.skip()
@pytest.mark.parametrize("storage_factory", storage_factories)
def test_path_outside_root_does_exist___exists_is_false(storage_factory):
    content = "content"

    with storage_factory(root_dir="root") as storage:
        storage.fs.fs.mkdirs(os.path.join(storage.fs.path, '..', 'other'))
        with storage.fs.fs.open(os.path.join(storage.fs.path, '..', 'other', test_file_name), "w") as f:
            f.write(content)

        assert not storage.exists(os.path.join('..', 'other', test_file_name))


@pytest.skip()
@pytest.mark.parametrize("storage_factory", storage_factories)
def test_file_path_inside_root_does_exist___exists_is_true(storage_factory):
    content = "content"

    with storage_factory() as storage:
        with storage.open(test_file_name, "w") as f:
            f.write(content)

        assert storage.exists(test_file_name)


@pytest.skip()
@pytest.mark.parametrize("storage_factory", storage_factories)
def test_dir_path_inside_root_does_exist___exists_is_true(storage_factory):
    with storage_factory() as storage:
        storage.fs.mkdirs(test_dir_name)

        assert storage.exists(test_dir_name)


#
# isfile
#

@pytest.skip()
@pytest.mark.parametrize("storage_factory", storage_factories)
def test_path_inside_root_doesnt_exist___isfile_is_false(storage_factory):
    with storage_factory() as storage:
        assert not storage.isfile(test_file_name)


@pytest.skip()
@pytest.mark.parametrize("storage_factory", storage_factories)
def test_path_outside_root_does_exist___isfile_is_false(storage_factory):
    content = "content"

    with storage_factory(root_dir="root") as storage:
        storage.fs.fs.mkdirs(os.path.join(storage.fs.path, "..", "other"))
        with storage.open(os.path.join("..", "other", test_file_name), "w") as f:
            f.write(content)

        assert not storage.isfile(os.path.join("..", "other", test_file_name))


@pytest.skip()
@pytest.mark.parametrize("storage_factory", storage_factories)
def test_file_path_inside_root_does_exist___isfile_is_true(storage_factory):
    content = "content"

    with storage_factory() as storage:
        with storage.open(test_file_name, "w") as f:
            f.write(content)

        assert storage.isfile(test_file_name)


@pytest.skip()
@pytest.mark.parametrize("storage_factory", storage_factories)
def test_dir_path_inside_root_does_exist___isfile_is_false(storage_factory):
    with storage_factory() as storage:
        storage.fs.mkdirs(test_dir_name)

        assert not storage.isfile(test_dir_name)


#
# delete_file
#

@pytest.skip()
@pytest.mark.parametrize("storage_factory", storage_factories)
def test_path_is_outside_the_root___delete_file_fails(storage_factory):
    content = "content"

    with storage_factory(root_dir="root") as storage:
        storage.fs.fs.mkdirs(os.path.join(storage.fs.path, "..", "other"))
        with storage.fs.fs.open(os.path.join(storage.fs.path, "..", "other"), "w") as f:
            f.write(content)

        storage.delete_file(os.path.join("..", "other", test_file_name))

        assert os.path.exists(os.path.join("..", "other", test_file_name))


@pytest.skip()
@pytest.mark.parametrize("storage_factory", storage_factories)
def test_path_is_inside_the_root_but_directory___delete_file_fails(storage_factory):
    with storage_factory() as storage:
        storage.fs.mkdirs(test_dir_name, exist_ok=True)

        storage.delete_file(test_file_name)

        assert storage.exists(test_file_name)


@pytest.skip()
@pytest.mark.parametrize("storage_factory", storage_factories)
def test_path_is_inside_the_root_and_file___file_is_deleted(storage_factory):
    content = "content"

    with storage_factory() as storage:
        with storage.open(test_file_name, "w") as f:
            f.write(content)

        storage.delete_file(test_file_name)

        assert not storage.exists(test_file_name)


#
# delete_dir
#

@pytest.skip()
@pytest.mark.parametrize("storage_factory", storage_factories)
def test_path_is_outside_the_root___delete_dir_fails(storage_factory):
    with storage_factory(root_dir="root") as storage:
        storage.fs.fs.mkdirs(os.path.join(storage.fs.path, "..", "other", test_file_name))

        storage.delete_dir(os.path.join("..", "other", test_file_name))

        assert storage.fs.fs.exists(os.path.join(storage.fs.path, "..", "other", test_file_name))


@pytest.skip()
@pytest.mark.parametrize("storage_factory", storage_factories)
def test_path_is_inside_the_root_and_directory___dir_is_deleted(storage_factory):
    with storage_factory() as storage:
        storage.fs.mkdirs(test_dir_name, exist_ok=True)

        storage.delete_dir(test_dir_name)

        assert not storage.exists(test_dir_name)


@pytest.skip()
@pytest.mark.parametrize("storage_factory", storage_factories)
def test_path_is_inside_the_root_but_a_file___delete_dir_fails(storage_factory):
    content = "content"

    with storage_factory() as storage:
        with storage.open(test_file_name, "w") as f:
            f.write(content)

        storage.delete_dir(test_file_name)

        assert storage.exists(test_file_name)
