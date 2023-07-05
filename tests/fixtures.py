import pytest

from lot3.df_engine.config import configure as engine_config, get_df_engine
from lot3.df_reader.config import configure as reader_config, get_df_reader


engine_configs = [
    {
        "default": {
            "path": "lot3.df_engine.base.BaseDfEngine",
            "options": {},
        }
    },
]


def with_each_df_engine(test):
    @pytest.mark.parametrize("config", engine_configs)
    def _test(config, *args, **kwargs):
        engine_config(config)
        engine = get_df_engine("default")
        test(*args, engine=engine, **kwargs)

    return _test


reader_configs = [
    {
        "default": {
            "path": "lot3.df_engine.base.BaseDfEngine",
            "options": {},
        }
    },
]


def with_each_df_readers(test):
    @pytest.mark.parametrize("config", engine_configs)
    def _test(config, *args, **kwargs):
        reader_config(config)
        engine = get_df_reader("default")
        test(*args, engine=engine, **kwargs)

    return _test
