from copy import deepcopy

from dask import dataframe

from .base import (BaseDfEngine, BaseOasisDataframe, BaseOasisSeries,
                   wrap_result)


class DaskOasisSeries(BaseOasisSeries):
    base = dataframe.Series


class DaskOasisDataframe(BaseOasisDataframe):
    base = dataframe.DataFrame


DaskOasisSeries.dataframe_class = DaskOasisDataframe
DaskOasisSeries.series_class = DaskOasisSeries
DaskOasisDataframe.dataframe_class = DaskOasisDataframe
DaskOasisDataframe.series_class = DaskOasisSeries


class DaskDfEngine(BaseDfEngine):
    module = dataframe
    dataframe_class = DaskOasisDataframe
    series_class = DaskOasisSeries

    @classmethod
    def read_csv(
        cls, *args, index=None, index_col=None, nrows=None, dtype=None, **kwargs
    ):
        index_col = index_col or index

        df = cls.module.read_csv(
            *args,
            dtype={
                "BITIV": "float64",
                "CondTag": "float64",
                "LocDed1Building": "float64",
                "LocDed3Contents": "float64",
                "LocDed4BI": "float64",
                "LocDed5PD": "float64",
                "LocLimit6All": "float64",
                "LocMinDed6All": "float64",
                "LocNumber": "object",
                "CondLimit6All": "float64",
                "LayerParticipation": "float64",
                "PolDed6All": "float64",
                "PolLimit6All": "float64",
                **(dtype or {}),
            },
            **kwargs
        )
        df = df.categorize()
        if index_col:
            df = df.set_index(index_col)
        if nrows:
            df = df.head(nrows=nrows)

        return cls.wrap_result(df)
