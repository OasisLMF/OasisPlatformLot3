from lot3.df_engine import configure, get_df_engine, BaseDfEngine


class TestDfEngine(BaseDfEngine):
    def __init__(self, *args, extra=None, **kwargs):
        self.extra = extra
        super().__init__(*args, **kwargs)


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
