import pytest

from lot3.df_engine import configure


@pytest.fixture(autouse=True)
def reset_wrapper_config():
    configure()


