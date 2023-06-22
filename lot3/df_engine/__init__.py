from .base import BaseDfEngine
from .config import get_df_engine, configure

configure()
pd = get_df_engine("default")
