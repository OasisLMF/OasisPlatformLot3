from copy import deepcopy

from pyspark import pandas

from .base import (BaseDfEngine, BaseOasisDataframe, BaseOasisSeries,
                   wrap_result)


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
