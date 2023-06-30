"""
    Readers to replace direct usage of pd.read_csv/read_parquet and allows for filters() & sql()
    to be provided.
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
from pyspark.sql.types import StructType

logger = logging.getLogger("lot3.df_reader.reader")


class OasisReader:
    """
    Base reader.

    as_pandas(), sql() & filter() can all be chained with self.read controlling whether the base
    read (read_csv/read_parquet) needs to be triggered. This is because in the case of spark
    we need to read differently depending on if the intention is to do sql or filter.
    """

    def __init__(self, filename_or_buffer, *args, **kwargs):
        self.filename_or_buffer = filename_or_buffer
        self.df = None
        self.applied_sql = False
        self.applied_filters = False
        self.read = False
        self.reader_args = args
        self.reader_kwargs = kwargs

    def _read(self):
        if not self.read:
            if hasattr(self, "read_csv"):
                self.read = True
                return self.read_csv(
                    self.filename_or_buffer, *self.reader_args, **self.reader_kwargs
                )
            elif hasattr(self, "read_parquet"):
                self.read = True
                return self.read_parquet(
                    self.filename_or_buffer, *self.reader_args, **self.reader_kwargs
                )

    def filter(self, filters):
        if filters:
            self.applied_filters = True
            self._read()

            for df_filter in filters:
                self.df = df_filter(self.df)
        return self

    def apply_filter(self, filters):
        pass

    def sql(self, sql):
        if sql:
            self.applied_sql = True
            self._read()
            self.apply_sql(sql)
        return self

    def apply_sql(self, sql):
        pass

    def as_pandas(self):
        self._read()


class OasisPandasReader(OasisReader):
    def as_pandas(self):
        super().as_pandas()
        return self.df


class OasisPandasReaderCSV(OasisPandasReader):
    def read_csv(self, *args, **kwargs):
        self.df = pd.read_csv(*args, **kwargs)


class OasisPandasReaderParquet(OasisPandasReader):
    def read_parquet(self, *args, **kwargs):
        self.df = pd.read_parquet(*args, **kwargs)


class OasisDaskReader(OasisReader):
    def apply_sql(self, sql):
        try:
            c = Context()
            # TODO should the table name be the csv filename, seems no harm in that unless
            # we have same name csv's elsewhere.
            table_name = os.path.basename(self.filename_or_buffer).split(".")[0]
            c.create_table(table_name, self.df)
            formatted_sql = sql.replace("table", table_name)

            pre_sql_columns = self.df.columns

            # dask expects the columns to be lower case, which won't match some data
            self.df = c.sql(
                formatted_sql,
                config_options={"sql.identifier.case_sensitive": False},
            )
            # which means we then need to map the columns back to the original
            self.df.columns = [
                x for x in pre_sql_columns if x.lower() in self.df.columns
            ]
        except ParsingException:
            # TODO - validation? need to store images for display when this is hooked up. Probably bubble
            # this up to a generic sql message
            logger.warning("Invalid SQL provided")
            # temp until we decide on handling, i.e don't return full data if it fails.
            self.df = dd.DataFrame.from_dict({}, npartitions=1)

    def as_pandas(self):
        super().as_pandas()
        return self.df.compute()


class OasisDaskReaderCSV(OasisDaskReader):
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

        self.df = dd.read_csv(filename_or_buffer, *args, **dask_safe_kwargs)


class OasisDaskReaderParquet(OasisDaskReader):
    def read_parquet(self, filename_or_buffer, *args, **kwargs):
        self.df = dd.read_parquet(filename_or_buffer, *args, **kwargs)

        # Currently categorical queries are not supported in dask https://github.com/dask-contrib/dask-sql/issues/423
        # When reading csv, these become strings anyway, so for now we convert to strings.
        category_cols = self.df.select_dtypes(include="category").columns
        for col in category_cols:
            self.df[col] = self.df[col].astype(str)


class OasisSparkReader(OasisReader):
    """
    While not yet prevented, currently sql and filter are not intended to be used together and in
    the case of spark, filter could only be used with sql if it was sql().as_pandas().filter() as they function
    on a different class.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.spark = SparkSession.builder.appName("OasisPlatformLot3").getOrCreate()

    def apply_sql(self, sql):
        try:
            table_name = os.path.basename(self.filename_or_buffer).split(".")[0]
            self.df.createOrReplaceTempView(table_name)
            self.df = self.spark.sql(sql.replace("table", table_name))
        except AnalysisException:
            # TODO - validation? need to store images for display when this is hooked up. Probably bubble
            # this up to a generic sql message
            logger.warning("Invalid SQL provided")
            # temp until we decide on handling, i.e don't return full data if it fails.
            self.df = self.spark.createDataFrame([], StructType([]))

    def as_pandas(self):
        super().as_pandas()
        if self.applied_sql:
            # read_csv (and dask) will only return strings, integers and float, everything else is an object that
            # becomes a string. Spark will return timestamps/datetimes leading to a difference, so force the
            # evaluation for now. We need to see how this impacts the other code I can't see any obvious uses
            # of the parse_dates etc in read_csv.
            timestamp_to_strings = []
            for col, col_type in self.df.dtypes:
                if col_type in ["timestamp_ntz", "date"]:
                    self.df = self.df.withColumn(col, self.df[col].cast("string"))
                    # the ones we convert back to datetime64[ns]
                    if col_type in ["timestamp_ntz"]:
                        timestamp_to_strings.append(col)

            df = self.df.toPandas()
        else:
            # as per above, different structure when pyspark.pandas vs sql
            timestamp_to_strings = []
            for col in self.df.select_dtypes(include="timestamp_ntz").columns:
                self.df[col] = self.df[col].astype(str)
                timestamp_to_strings.append(col)

            # as per above, different structure when pyspark.pandas vs sql
            for col in self.df.select_dtypes(include="object").columns:
                self.df[col] = self.df[col].astype(str)

            df = self.df.to_pandas()
        return df.astype({col: "datetime64[ns]" for col in timestamp_to_strings})


class OasisSparkReaderCSV(OasisSparkReader):
    def read_csv(self, filename_or_buffer, *args, **kwargs):
        if not self.applied_sql:
            self.df = ps.read_csv(filename_or_buffer, *args, **kwargs)
        else:
            self.df = self.spark.read.csv(
                filename_or_buffer, header=True, inferSchema=True
            )


class OasisSparkReaderParquet(OasisSparkReader):
    def read_parquet(self, filename_or_buffer, *args, **kwargs):
        if not self.applied_sql:
            self.df = ps.read_parquet(filename_or_buffer, *args, **kwargs)
        else:
            self.df = self.spark.read.parquet(filename_or_buffer)

        return self.df
