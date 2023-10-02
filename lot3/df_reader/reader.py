"""
    Readers to replace direct usage of pd.read_csv/read_parquet and allows for filters() & sql()
    to be provided.
"""

import io
import logging
import pathlib
from typing import Iterable

import dask
import dask_geopandas as dgpd
import geopandas as gpd
import pandas as pd
from dask import dataframe as dd
from dask_sql import Context
from dask_sql.utils import ParsingException
from distributed import Client

from ..filestore.backends.storage_manager import BaseStorageConnector
from .exceptions import InvalidSQLException

dask.config.set(
    {"dataframe.convert-string": False}
)  # allows dask sql to support pyarrow
logger = logging.getLogger("lot3.df_reader.reader")


class OasisReader:
    """
    Base reader.

    as_pandas(), sql() & filter() can all be chained with self.has_read controlling whether the base
    read (read_csv/read_parquet) needs to be triggered. This is because in the case of spark
    we need to read differently depending on if the intention is to do sql or filter.
    """

    def __init__(
        self,
        filename_or_buffer,
        storage: BaseStorageConnector,
        *args,
        dataframe=None,
        has_read=False,
        **kwargs
    ):
        self.filename_or_buffer = filename_or_buffer

        self.storage = storage
        self._df = dataframe
        self.has_read = has_read
        self.reader_args = args
        self.reader_kwargs = kwargs

        if isinstance(
            self.filename_or_buffer, str
        ) and self.filename_or_buffer.lower().endswith(".zip"):
            self.reader_kwargs["compression"] = "zip"

    @property
    def df(self):
        self._read()
        return self._df

    @df.setter
    def df(self, other):
        self._df = other

    def read_csv(self, *args, **kwargs):
        raise NotImplementedError()

    def read_parquet(self, *args, **kwargs):
        raise NotImplementedError()

    def _read(self):
        if not self.has_read:
            if hasattr(self.filename_or_buffer, "name"):
                extension = pathlib.Path(self.filename_or_buffer.name).suffix
            else:
                extension = pathlib.Path(self.filename_or_buffer).suffix

            if extension in [".parquet", ".pq"]:
                self.has_read = True
                self.read_parquet(*self.reader_args, **self.reader_kwargs)
            else:
                # assume the file is csv if not parquet
                self.has_read = True
                self.read_csv(*self.reader_args, **self.reader_kwargs)

        return self

    def copy_with_df(self, df):
        return type(self)(
            self.filename_or_buffer, self.storage, dataframe=df, has_read=self.has_read
        )

    def filter(self, filters):
        self._read()

        df = self.df
        for df_filter in filters if isinstance(filters, Iterable) else [filters]:
            df = df_filter(df)

        return self.copy_with_df(df)

    def apply_filter(self, filters):
        pass

    def sql(self, sql):
        if sql:
            self._read()
            return self.apply_sql(sql)
        return self

    def query(self, fn):
        return fn(self.df)

    def as_pandas(self):
        self._read()
        return self.df


class OasisPandasReader(OasisReader):
    def read_csv(self, *args, **kwargs):
        if isinstance(self.filename_or_buffer, str):
            if self.filename_or_buffer.startswith(
                "http://"
            ) or self.filename_or_buffer.startswith("https://"):
                self.df = pd.read_csv(self.filename_or_buffer, *args, **kwargs)
            else:
                _, uri = self.storage.get_storage_url(
                    self.filename_or_buffer, encode_params=False
                )
                self.df = pd.read_csv(
                    uri,
                    *args,
                    **kwargs,
                    storage_options=self.storage.get_fsspec_storage_options(),
                )
        else:
            self.df = pd.read_csv(self.filename_or_buffer, *args, **kwargs)

    def read_parquet(self, *args, **kwargs):
        if isinstance(self.filename_or_buffer, str):
            if self.filename_or_buffer.startswith(
                "http://"
            ) or self.filename_or_buffer.startswith("https://"):
                self.df = pd.read_parquet(self.filename_or_buffer, *args, **kwargs)
            else:
                _, uri = self.storage.get_storage_url(
                    self.filename_or_buffer, encode_params=False
                )
                self.df = pd.read_parquet(
                    uri,
                    *args,
                    **kwargs,
                    storage_options=self.storage.get_fsspec_storage_options(),
                )
        else:
            self.df = pd.read_parquet(self.filename_or_buffer, *args, **kwargs)

    def apply_geo(self, shape_filename_path, *args, drop_geo=True, **kwargs):
        """
        Read in a shape file and return the _read file with geo data joined.
        """
        # TODO: fix this so that it can work with non local files
        # with self.storage.open(self.shape_filename_path) as f:
        #     shape_df = gpd.read_file(f)

        shape_df = gpd.read_file(shape_filename_path)

        # for situations where the columns in the source data are different.
        lon_col = kwargs.get("geo_lon_col", "longitude")
        lat_col = kwargs.get("geo_lat_col", "latitude")

        df_columns = self.df.columns.tolist()
        if lat_col not in df_columns or lon_col not in df_columns:
            logger.warning("Invalid shape file provided")
            # temp until we decide on handling, i.e don't return full data if it fails.
            return self.copy_with_df(pd.DataFrame.from_dict({}))

        # convert read df to geo
        df = gpd.GeoDataFrame(
            self.df, geometry=gpd.points_from_xy(self.df[lon_col], self.df[lat_col])
        )

        # Make sure they're using the same projection reference
        df.crs = shape_df.crs

        # join the datasets, matching `geometry` to points within the shape df
        df = df.sjoin(shape_df, how="inner")

        if drop_geo:
            df = df.drop(shape_df.columns.tolist() + ["index_right"], axis=1)

        return self.copy_with_df(df)


class OasisPandasReaderCSV(OasisPandasReader):
    pass


class OasisPandasReaderParquet(OasisPandasReader):
    pass


class OasisDaskReader(OasisReader):
    def __init__(self, *args, client_address=None, **kwargs):
        if client_address:
            self.client = Client(client_address, set_as_default=False)
        else:
            self.client = None

        super().__init__(*args, **kwargs)

    def copy_with_df(self, df):
        res = super().copy_with_df(df)
        res.client = self.client
        return res

    def apply_geo(self, shape_filename_path, *args, drop_geo=True, **kwargs):
        """
        Read in a shape file and return the _read file with geo data joined.
        """
        # TODO: fix this so that it can work with non local files
        # with self.storage.open(self.shape_filename_path) as f:
        #     shape_df = dgpd.read_file(f, npartitions=1)

        shape_df = dgpd.read_file(shape_filename_path, npartitions=1)

        # for situations where the columns in the source data are different.
        lon_col = kwargs.get("geo_lon_col", "longitude")
        lat_col = kwargs.get("geo_lat_col", "latitude")

        df_columns = self.df.columns.tolist()
        if lat_col not in df_columns or lon_col not in df_columns:
            logger.warning("Invalid shape file provided")
            # temp until we decide on handling, i.e don't return full data if it fails.
            return self.copy_with_df(dd.DataFrame.from_dict({}, npartitions=1))

        df = self.df.copy()

        # convert read df to geo
        df["geometry"] = dgpd.points_from_xy(df, lon_col, lat_col)
        df = dgpd.from_dask_dataframe(df)

        # Make sure they're using the same projection reference
        df.crs = shape_df.crs

        # join the datasets, matching `geometry` to points within the shape df
        df = df.sjoin(shape_df, how="inner")

        if drop_geo:
            df = df.drop(shape_df.columns.tolist() + ["index_right"], axis=1)

        return self.copy_with_df(df)

    def apply_sql(self, sql):
        df = self.df.copy()

        try:
            c = Context()

            # Initially this was the filename, but some filenames are invalid for the table,
            # is it ok to call it the same name all the time? Mapped to DaskDataTable in case
            # we need to change this.
            c.create_table("DaskDataTable", self.df)
            formatted_sql = sql.replace("table", "DaskDataTable")

            pre_sql_columns = df.columns

            # dask expects the columns to be lower case, which won't match some data
            df = c.sql(
                formatted_sql,
                config_options={"sql.identifier.case_sensitive": False},
            )
            # which means we then need to map the columns back to the original
            # and allow for any aggregations to be retained
            validated_columns = []
            for v in df.columns:
                pre = False
                for x in pre_sql_columns:
                    if v.lower() == x.lower():
                        validated_columns.append(x)
                        pre = True

                if not pre:
                    validated_columns.append(v)
            df.columns = validated_columns

            return self.copy_with_df(df)
        except ParsingException:
            raise InvalidSQLException

    def as_pandas(self):
        super().as_pandas()
        if self.client:
            return self.client.compute(self.df).result()
        else:
            return self.df.compute()

    def read_csv(self, *args, **kwargs):
        # remove standard pandas kwargs which will case an issue in dask.
        dask_safe_kwargs = kwargs.copy()
        dask_safe_kwargs.pop("memory_map", None)
        dask_safe_kwargs.pop("low_memory", None)

        filename_or_buffer = self.filename_or_buffer
        if isinstance(filename_or_buffer, pathlib.PosixPath):
            filename_or_buffer = str(self.filename_or_buffer)

        if isinstance(filename_or_buffer, io.TextIOWrapper) or isinstance(
            filename_or_buffer, io.BufferedReader
        ):
            filename_or_buffer = filename_or_buffer.name

        # django files
        if hasattr(filename_or_buffer, "path"):
            filename_or_buffer = filename_or_buffer.path

        _, uri = self.storage.get_storage_url(filename_or_buffer, encode_params=False)
        self.df = dd.read_csv(
            uri,
            *args,
            **dask_safe_kwargs,
            storage_options=self.storage.get_fsspec_storage_options(),
        )

    def read_parquet(self, *args, **kwargs):
        if isinstance(self.filename_or_buffer, str):
            _, uri = self.storage.get_storage_url(
                self.filename_or_buffer, encode_params=False
            )
            self.df = dd.read_parquet(
                uri,
                *args,
                **kwargs,
                storage_options=self.storage.get_fsspec_storage_options(),
            )
        else:
            self.df = dd.read_parquet(
                self.filename_or_buffer,
                *args,
                **kwargs,
            )

        category_cols = self.df.select_dtypes(include="category").columns
        for col in category_cols:
            self.df[col] = self.df[col].astype(self.df[col].dtype.categories.dtype)


class OasisDaskReaderCSV(OasisDaskReader):
    pass


class OasisDaskReaderParquet(OasisDaskReader):
    pass


# Spark reader paused.
# class OasisSparkReader(OasisReader):
#     """
#     While not yet prevented, currently sql and filter are not intended to be used together and in
#     the case of spark, filter could only be used with sql if it was sql().as_pandas().filter() as they function
#     on a different class.
#     """
#
#     def __init__(self, *args, **kwargs):
#         super().__init__(*args, **kwargs)
#         self.spark = SparkSession.builder.appName("OasisPlatformLot3").getOrCreate()
#
#     def apply_geo(self, *args, **kwargs):
#         """
#         TODO - this is where we paused the Spark integration.
#         """
#         # import geopandas as gpd
#         # from shapely.geometry import MultiPolygon
#         #
#         # shape_df = gpd.read_file(self.shape_filename_path)
#         # # shape_df['longitude'] = shape_df['geometry'].x
#         # # shape_df['latitude'] = shape_df['geometry'].y
#         # # shape_df_spark = self.spark.createDataFrame(pd.DataFrame(shape_df).drop(['geometry'], axis=1))
#         # # shape_df_spark_broadcast = self.spark.sparkContext.broadcast(shape_df_spark)
#         #
#         # @pandas_udf({}, PandasUDFType.GROUPED_MAP)
#         # def join_shape_df_broadcast(data):
#         #     # shapes = gpd.GeoDataFrame(data, geometry=gpd.points_from_xy(shape_df_spark_broadcast['longitude'], shape_df_spark_broadcast['latitude']))
#         #     data = gpd.GeoDataFrame(data, geometry=gpd.points_from_xy(data['longitude'], data['latitude']))
#         #     joined_df = gpd.sjoin(data, shape_df, how='inner')
#         #     return joined_df
#
#         # self.df = self.df.groupby("").apply(join_shape_df_broadcast)
#
#     def apply_sql(self, sql):
#         try:
#             table_name = os.path.basename(self.filename_or_buffer).split(".")[0]
#             self.df.createOrReplaceTempView(table_name)
#             self.df = self.spark.sql(sql.replace("table", table_name))
#         except AnalysisException:
#             # TODO - validation? need to store images for display when this is hooked up. Probably bubble
#             # this up to a generic sql message
#             logger.warning("Invalid SQL provided")
#             # temp until we decide on handling, i.e don't return full data if it fails.
#             self.df = self.spark.createDataFrame([], StructType([]))
#
#     def as_pandas(self):
#         super().as_pandas()
#         # read_csv (and dask) will only return strings, integers and float, everything else is an object that
#         # becomes a string. Spark will return timestamps/datetimes leading to a difference, so force the
#         # evaluation for now. We need to see how this impacts the other code I can't see any obvious uses
#         # of the parse_dates etc in read_csv.
#         timestamp_to_strings = []
#         for col, col_type in self.df.dtypes:
#             if col_type in ["timestamp_ntz", "date"]:
#                 self.df = self.df.withColumn(col, self.df[col].cast("string"))
#                 # the ones we convert back to datetime64[ns]
#                 if col_type in ["timestamp_ntz"]:
#                     timestamp_to_strings.append(col)
#
#         df = self.df.toPandas()
#         return df.astype({col: "datetime64[ns]" for col in timestamp_to_strings})
#
#
# class OasisSparkReaderCSV(OasisSparkReader):
#     def read_csv(self, filename_or_buffer, *args, **kwargs):
#         self.df = self.spark.read.csv(filename_or_buffer, header=True, inferSchema=True)
#
#
# class OasisSparkReaderParquet(OasisSparkReader):
#     def read_parquet(self, filename_or_buffer, *args, **kwargs):
#         self.df = self.spark.read.parquet(filename_or_buffer)
