from copy import deepcopy

from .base import BaseDfEngine, BaseOasisSeries, BaseOasisDataframe, wrap_result
from pyspark import pandas


class SparkOasisSeries(BaseOasisSeries):
    base = pandas.Series


class SparkOasisDataframe(BaseOasisDataframe):
    base = pandas.DataFrame


SparkOasisSeries.dataframe_class = SparkOasisDataframe
SparkOasisSeries.series_class = SparkOasisSeries
SparkOasisDataframe.dataframe_class = SparkOasisDataframe
SparkOasisDataframe.series_class = SparkOasisSeries


class SparkDfEngine(BaseDfEngine):
    module = pandas
    dataframe_class = SparkOasisDataframe
    series_class = SparkOasisSeries

    @classmethod
    def read_csv(cls, path, *args, **kwargs):
        spark_kwargs = deepcopy(kwargs)
        if isinstance(spark_kwargs.get("index_col"), bool):
            spark_kwargs.pop("index_col")

        return cls.wrap_result(cls.module.read_csv(str(path), *args, **spark_kwargs))
