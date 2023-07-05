import pytest

from lot3.df_reader.config import configure, get_df_reader, OasisReader
from lot3.df_reader.config import ConfigError
from lot3.df_reader.reader import OasisPandasReader


class TestDfReader(OasisReader):
    def __init__(self, *args, extra=None, **kwargs):
        self.extra = extra
        super().__init__(*args, **kwargs)


class NotDfReader:
    pass


config = {
    "default": {
        "path": "lot3.df_reader.reader.OasisPandasReader",
        "options": {},
    },
    "test": {
        "path": "tests.df_reader.test_config.TestDfReader",
        "options": {
            "extra": "test value",
        },
    },
}


def test_no_configuration_is_set___default_class_is_used():
    configure()
    reader = get_df_reader("default", "testpath")

    assert type(reader) is OasisPandasReader


def test_configuration_is_set_default_is_requested___default_class_is_used():
    configure(config)
    reader = get_df_reader("default", "testpath")

    assert type(reader) is OasisPandasReader


def test_configuration_is_set_existing_type_is_requested___correct_class_with_args_is_used():
    configure(config)
    reader = get_df_reader("test", "testpath")

    assert type(reader) is TestDfReader
    assert reader.extra == "test value"


def test_configuration_is_set_missing_type_is_requested___default_class_is_used():
    configure(config)
    reader = get_df_reader("other", "testpath")

    assert type(reader) is OasisPandasReader


def test_config_not_present_and_no_default___error_is_raised():
    configure({
        "test": {
            "path": "tests.df_reader.test_config.TestDfReader",
            "options": {
                "extra": "test value",
            },
        },
    })

    with pytest.raises(ConfigError) as exec_info:
        get_df_reader("other", "testpath")

    assert str(exec_info.value) == "'other' was not fund in the df_reader configuration and no 'default' was provided"


def test_path_not_present_in_config___error_is_raised():
    configure({
        "test": {
            "options": {
                "extra": "test value",
            },
        },
    })

    with pytest.raises(ConfigError) as exec_info:
        get_df_reader("test", "testpath")

    assert str(exec_info.value) == "'path' not found in the df_reader config for 'test' files"


def test_path_is_not_absolute_path___error_is_raised():
    configure({
        "test": {
            "path": "path_to_a_module",
            "options": {
                "extra": "test value",
            },
        },
    })

    with pytest.raises(ConfigError) as exec_info:
        get_df_reader("test", "testpath")

    assert str(exec_info.value) == (
        "'path' found in the df_reader config for 'test' files is not valid. It should be the "
        "absolute python path to the class to use eg. lot3.df_reader.reader.OasisReader"
    )


def test_module_doesnt_exist___error_is_raised():
    configure({
        "test": {
            "path": "tests.df_reader.test_config_other.TestDfReader",
            "options": {
                "extra": "test value",
            },
        },
    })

    with pytest.raises(ImportError):
        get_df_reader("test", "testpath")


def test_class_doesnt_exist_in_module___error_is_raised():
    configure({
        "test": {
            "path": "tests.df_reader.test_config.OtherDfReader",
            "options": {
                "extra": "test value",
            },
        },
    })

    with pytest.raises(AttributeError):
        get_df_reader("test", "testpath")


def test_reader_isnt_an_instance_of_the_base_reader____error_is_raised():
    configure({
        "test": {
            "path": "tests.df_reader.test_config.NotDfReader",
            "options": {},
        },
    })

    with pytest.raises(ConfigError) as exec_info:
        get_df_reader("test", "testpath")

    assert str(exec_info.value) == "'NotDfReader' does not extend 'OasisReader'"
