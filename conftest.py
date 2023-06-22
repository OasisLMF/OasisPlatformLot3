import pytest

from lot3.df_engine.config import reset


@pytest.fixture(autouse=True)
def reset_wrapper_config():
    reset()


