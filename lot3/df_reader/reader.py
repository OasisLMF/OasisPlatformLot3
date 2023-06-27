"""
    Wrapper for read_csv to make Pandas, Dask and Spark swappable and filterable.
"""
import io
import logging
import os
import pathlib

import pandas as pd
from dask import dataframe as dd
from dask_sql import Context
from dask_sql.utils import ParsingException

logger = logging.getLogger("lot3.df_reader.reader")


class OasisReader:
    def __init__(self, df_filters=None, sql=None):
        self.df_filters = df_filters
        self.sql = sql

    def apply_df_filters(self, df):
        for df_filter in self.df_filters:
            df = df_filter(df)
        return df


class OasisPandasReader(OasisReader):
    """
    Thin wrapper for pandas, to apply func based filters to dataframes following read_*.
    """

    engine = pd

    def read_csv(self, *args, **kwargs):
        df = self.engine.read_csv(*args, **kwargs)

        if self.df_filters:
            df = self.apply_df_filters(df)

        return df


class OasisDaskReader(OasisReader):
    """
    Thin wrapper for dask, to apply func based filters or SQL filtering to dataframes following read_*.

    read_* returns a computed dask dataframe, i.e a pandas dataframe for use in other packages.
    """

    engine = dd

    @classmethod
    def get_table_name_from_path(cls, filename_or_buffer):
        return os.path.basename(filename_or_buffer)

    def read_csv(self, filename_or_buffer, *args, **kwargs):
        # remove standard pandas kwargs which will case an issue in dask.
        dask_safe_kwargs = kwargs.copy()
        dask_safe_kwargs.pop("memory_map", None)
        dask_safe_kwargs.pop("low_memory", None)

        if isinstance(filename_or_buffer, pathlib.PosixPath):
            filename_or_buffer = str(filename_or_buffer)

        if isinstance(filename_or_buffer, io.TextIOWrapper) or isinstance(
            filename_or_buffer, io.BufferedReader
        ):
            filename_or_buffer = filename_or_buffer.name

        if filename_or_buffer.endswith(".zip"):
            kwargs["compression"] = None

        df = self.engine.read_csv(filename_or_buffer, *args, **dask_safe_kwargs)

        if self.df_filters:
            df = self.apply_df_filters(df)

        if self.sql:
            try:
                c = Context()
                # TODO should the tablename be the csv filename, seems no harm in that unless
                # we have same name csv's elsewhere.
                table_name = self.get_table_name_from_path(filename_or_buffer).split(
                    "."
                )[0]
                c.create_table(table_name, df)
                formatted_sql = self.sql.replace("table", table_name)

                # run the sql, removing case_sensitivity
                df = c.sql(
                    formatted_sql,
                    config_options={"sql.identifier.case_sensitive": False},
                )
            except ParsingException:
                # TODO - would be nice if we could validate/provide errors, the errors that come back
                # are not very helpful for debugging SQL
                # TODO - guess this will return in the CLI
                logger.warning("Invalid SQL provided")
                return None

        # return as a pandas dataframe
        return df.compute()


class OasisSparkReader:
    pass  # TODO
