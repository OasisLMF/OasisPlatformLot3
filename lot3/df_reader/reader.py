"""
    Wrapper for read_csv to make Pandas, Dask and Spark swappable and filterable.
"""
import io
import logging
import os
import pathlib

import pandas as pd
import pyspark.pandas as ps
from dask import dataframe as dd
from dask_sql import Context
from dask_sql.utils import ParsingException
from pyspark.errors import AnalysisException
from pyspark.sql import SparkSession

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

    def apply_sql(self, df, filename_or_buffer):
        try:
            c = Context()
            # TODO should the tablename be the csv filename, seems no harm in that unless
            # we have same name csv's elsewhere.
            table_name = os.path.basename(filename_or_buffer).split(".")[0]
            c.create_table(table_name, df)
            formatted_sql = self.sql.replace("table", table_name)

            # spark expects the columns to be lower case, which won't match some data
            # force it to ignore case
            sql_df = c.sql(
                formatted_sql,
                config_options={"sql.identifier.case_sensitive": False},
            )
            # which means we then need to map the columns back to the original
            sql_df.columns = [x for x in df.columns if x.lower() in sql_df.columns]
            return sql_df
        except ParsingException:
            # TODO - would be nice if we could validate/provide errors, the errors that come back
            # are not very helpful for debugging SQL
            # TODO - guess this will return in the CLI
            logger.warning("Invalid SQL provided")
            return False

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
            df = self.apply_sql(df, filename_or_buffer)
            if df is False:
                return None

        # return as a pandas dataframe
        return df.compute()


class OasisSparkReader(OasisReader):
    engine = ps

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.spark = SparkSession.builder.appName("OasisPlatformLot3").getOrCreate()

    def apply_sql(self, df, filename_or_buffer):
        try:
            table_name = os.path.basename(filename_or_buffer).split(".")[0]
            df.createOrReplaceTempView(table_name)
            return self.spark.sql(self.sql.replace("table", table_name))
        except AnalysisException:
            logger.warning("Invalid SQL provided")
            return False

    def read_csv(self, filename_or_buffer, *args, **kwargs):
        df = self.spark.read.csv(filename_or_buffer, header=True, inferSchema=True)
        # TODO when there is no SQL we will get nothing, it might be better to use pyspark.pandas.sql
        # but then we can't combine filter with sql, depends if that is needed.
        if not self.sql:
            self.sql = "SELECT * FROM table"

        df = self.apply_sql(df, filename_or_buffer)

        if not df:
            return None

        df = df.toPandas()

        # read_csv (and dask) will only return strings, integers and float, everything else is an object that
        # becomes a string. Spark will return timestamps/datetimes leading to a difference, so force the
        # evaluation for now. We need to see how this impacts the other code I can't see any obvious uses
        # of the parse_dates etc in read_csv.
        object_cols = df.select_dtypes(include="object").columns
        df[object_cols] = df[object_cols].fillna("").astype(str)

        # standard filters will need to be after conversion back to pandas for spark.
        if self.df_filters:
            df = self.apply_df_filters(df)

        return df
