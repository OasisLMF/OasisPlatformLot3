import pytest

from lot3.df_engine.base import BaseDfEngine
from lot3.df_engine.config import configure, get_df_engine, ConfigError


class TestDfEngine(BaseDfEngine):
    def __init__(self, *args, extra=None, **kwargs):
        self.extra = extra
        super().__init__(*args, **kwargs)


class NotDfEngine:
    pass


config = {
    "default": {
        "path": "lot3.df_engine.base.BaseDfEngine",
        "options": {},
    },
    "test": {
        "path": "tests.df_engine.test_config.TestDfEngine",
        "options": {
            "extra": "test value",
        },
    },
}


def test_no_configuration_is_set___default_class_is_used():
    configure()
    engine = get_df_engine("default")

    assert type(engine) is BaseDfEngine


def test_configuration_is_set_default_is_requested___default_class_is_used():
    configure(config)
    engine = get_df_engine("default")

    assert type(engine) is BaseDfEngine


def test_configuration_is_set_existing_type_is_requested___correct_class_with_args_is_used():
    configure(config)
    engine = get_df_engine("test")

    assert type(engine) is TestDfEngine
    assert engine.extra == "test value"


def test_configuration_is_set_missing_type_is_requested___default_class_is_used():
    configure(config)
    engine = get_df_engine("other")

    assert type(engine) is BaseDfEngine


def test_config_not_present_and_no_default___error_is_raised():
    configure({
        "test": {
            "path": "tests.df_engine.test_config.TestDfEngine",
            "options": {
                "extra": "test value",
            },
        },
    })

    with pytest.raises(ConfigError) as exec_info:
        get_df_engine("other")

    assert str(exec_info.value) == "'other' was not fund in the df_engine configuration and no 'default' was provided"


def test_path_not_present_in_config___error_is_raised():
    configure({
        "test": {
            "options": {
                "extra": "test value",
            },
        },
    })

    with pytest.raises(ConfigError) as exec_info:
        get_df_engine("test")

    assert str(exec_info.value) == "'path' not found in the df_engine config for 'test' files"


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
        get_df_engine("test")

    assert str(exec_info.value) == (
        "'path' found in the df_engine config for 'test' files is not valid. It should be the "
        "absolute python path to the class to use eg. lot3.df_engine.base.BaseDfEngine"
    )


def test_module_doesnt_exist___error_is_raised():
    configure({
        "test": {
            "path": "tests.df_engine.test_config_other.TestDfEngine",
            "options": {
                "extra": "test value",
            },
        },
    })

    with pytest.raises(ImportError):
        get_df_engine("test")


def test_class_doesnt_exist_in_module___error_is_raised():
    configure({
        "test": {
            "path": "tests.df_engine.test_config.OtherDfEngine",
            "options": {
                "extra": "test value",
            },
        },
    })

    with pytest.raises(AttributeError):
        get_df_engine("test")


def test_engine_isnt_an_instance_of_the_base_engine____error_is_raised():
    configure({
        "test": {
            "path": "tests.df_engine.test_config.NotDfEngine",
            "options": {},
        },
    })

    with pytest.raises(ConfigError) as exec_info:
        get_df_engine("test")

    assert str(exec_info.value) == "'NotDfEngine' does not extend 'BaseDfEngine'"
