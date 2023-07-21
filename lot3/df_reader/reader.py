"""
    Readers to replace direct usage of pd.read_csv/read_parquet and allows for filters() & sql()
    to be provided.
"""

import io
import logging
import pathlib

import dask_geopandas as dgpd
import geopandas as gpd
import pandas as pd
from dask import dataframe as dd
from dask_sql import Context
from dask_sql.utils import ParsingException

from .exceptions import InvalidSQLException

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
        shape_filename_path=None,
        drop_geo=True,
        *args,
        **kwargs
    ):
        self.filename_or_buffer = filename_or_buffer
        self.shape_filename_path = shape_filename_path
        self.drop_geo = drop_geo
        self.df = None
        self.applied_sql = False
        self.applied_filters = False
        self.has_read = False
        self.applied_geo = False
        self.reader_args = args
        self.reader_kwargs = kwargs

    def read_csv(self, filepath, *args, **kwargs):
        raise NotImplementedError()

    def read_parquet(self, filepath, *args, **kwargs):
        raise NotImplementedError()

    def _read(self):
        if not self.has_read:
            if hasattr(self.filename_or_buffer, "name"):
                extension = pathlib.Path(self.filename_or_buffer.name).suffix
            else:
                extension = pathlib.Path(self.filename_or_buffer).suffix

            if extension == ".parquet":
                self.has_read = True
                self.read_parquet(self.filename_or_buffer, *self.reader_args, **self.reader_kwargs)
            else:
                # assume the file is csv if not parquet
                self.has_read = True
                self.read_csv(self.filename_or_buffer, *self.reader_args, **self.reader_kwargs)

            if self.shape_filename_path:
                self.applied_geo = True
                self.apply_geo(*self.reader_args, **self.reader_kwargs)

        return self

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

    def as_pandas(self):
        self._read()
        return self.df


class OasisPandasReader(OasisReader):
    def read_csv(self, *args, **kwargs):
        _args = args
        _kwargs = kwargs
        self.df = pd.read_csv(*args, **kwargs)

    def read_parquet(self, *args, **kwargs):
        self.df = pd.read_parquet(*args, **kwargs)

    def apply_geo(self, *args, **kwargs):
        """
        Read in a shape file and return the _read file with geo data joined.
        """
        shape_df = gpd.read_file(self.shape_filename_path)

        # for situations where the columns in the source data are different.
        lon_col = kwargs.get("geo_lon_col", "longitude")
        lat_col = kwargs.get("geo_lat_col", "latitude")

        df_columns = self.df.columns.tolist()
        if lat_col not in df_columns or lon_col not in df_columns:
            logger.warning("Invalid shape file provided")
            # temp until we decide on handling, i.e don't return full data if it fails.
            self.df = pd.DataFrame.from_dict({})
            return

        # convert read df to geo
        self.df = gpd.GeoDataFrame(
            self.df, geometry=gpd.points_from_xy(self.df[lon_col], self.df[lat_col])
        )

        # Make sure they're using the same projection reference
        self.df.crs = shape_df.crs

        # join the datasets, matching `geometry` to points within the shape df
        self.df = self.df.sjoin(shape_df, how="inner")

        if self.drop_geo:
            self.df = self.df.drop(shape_df.columns.tolist() + ["index_right"], axis=1)


class OasisPandasReaderCSV(OasisPandasReader):
    pass


class OasisPandasReaderParquet(OasisPandasReader):
    pass


class OasisDaskReader(OasisReader):
    def apply_geo(self, *args, **kwargs):
        """
        Read in a shape file and return the _read file with geo data joined.
        """
        shape_df = dgpd.read_file(self.shape_filename_path, npartitions=1)

        # for situations where the columns in the source data are different.
        lon_col = kwargs.get("geo_lon_col", "longitude")
        lat_col = kwargs.get("geo_lat_col", "latitude")

        df_columns = self.df.columns.tolist()
        if lat_col not in df_columns or lon_col not in df_columns:
            logger.warning("Invalid shape file provided")
            # temp until we decide on handling, i.e don't return full data if it fails.
            self.df = dd.DataFrame.from_dict({}, npartitions=1)
            return

        # convert read df to geo
        self.df["geometry"] = dgpd.points_from_xy(self.df, lon_col, lat_col)
        self.df = dgpd.from_dask_dataframe(self.df)

        # Make sure they're using the same projection reference
        self.df.crs = shape_df.crs

        # join the datasets, matching `geometry` to points within the shape df
        self.df = self.df.sjoin(shape_df, how="inner")

        if self.drop_geo:
            self.df = self.df.drop(shape_df.columns.tolist() + ["index_right"], axis=1)

    def apply_sql(self, sql):
        try:
            c = Context()
            # Initially this was the filename, but some filenames are invalid for the table,
            # is it ok to call it the same name all the time? Mapped to DaskDataTable in case
            # we need to change this.
            c.create_table("DaskDataTable", self.df)
            formatted_sql = sql.replace("table", "DaskDataTable")

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
            raise InvalidSQLException

    def as_pandas(self):
        super().as_pandas()
        return self.df.compute()

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

        # django files
        if hasattr(filename_or_buffer, "path"):
            filename_or_buffer = filename_or_buffer.path

        self.df = dd.read_csv(filename_or_buffer, *args, **dask_safe_kwargs)

    def read_parquet(self, filename_or_buffer, *args, **kwargs):
        self.df = dd.read_parquet(filename_or_buffer, *args, **kwargs)

        # Currently categorical queries are not supported in dask https://github.com/dask-contrib/dask-sql/issues/423
        # When reading csv, these become strings anyway, so for now we convert to strings.
        category_cols = self.df.select_dtypes(include="category").columns
        for col in category_cols:
            self.df[col] = self.df[col].astype(str)


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
