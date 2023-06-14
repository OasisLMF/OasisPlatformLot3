from typing import Callable

import numpy as np
import pandas as pd


def wrap_result(res, dataframe_class, series_class):
    if isinstance(res, Callable):
        return WrappedIndexAndCallable(res, dataframe_class, series_class)
    elif isinstance(res, dataframe_class.base):
        return dataframe_class(res)
    elif isinstance(res, series_class.base):
        return series_class(res)

    return res


class WrappedIndexAndCallable:
    def __init__(self, base_object, dataframe_class, series_class):
        self.base_object = base_object
        self.dataframe_class = dataframe_class
        self.series_class = series_class

    def __getitem__(self, item):
        return wrap_result(self.base_object[item], self.dataframe_class, self.series_class)

    def __setitem__(self, key, value):
        self.base_object[key] = base_v(value)

    def __call__(self, *args, **kwargs):
        processed_args = [base_v(arg) for arg in args]
        processed_kwargs = {k: base_v(v) for k, v in kwargs.items()}

        return wrap_result(
            self.base_object(*processed_args, **processed_kwargs),
            self.dataframe_class,
            self.series_class,
        )


def base_v(v):
    return getattr(v, "base_object", v)


class ResultsWrapperMeta(type):
    def __getattr__(cls, item):
        res = getattr(cls.base, item)
        return wrap_result(res, cls.dataframe_class, cls.series_class)


class ResultWrapper(metaclass=ResultsWrapperMeta):
    BLANK_VALUES = {np.nan, '', None, pd.NA, pd.NaT}

    def __init__(self, data=None, **kwargs):
        processed_kwargs = {k: base_v(v) for k, v in kwargs.items()}
        self.base_object = data if isinstance(data, self.base) else self.base(data=data, **processed_kwargs)

    def __getattr__(self, item):
        attr = getattr(self.base_object, item)
        return self.wrap_result(attr)

    def __setattr__(self, key, value):
        if key == "base_object":
            super().__setattr__(key, value)
        else:
            setattr(self.base_object, key, base_v(value))

    def __getitem__(self, item):
        attr = self.base_object[base_v(item)]
        return self.wrap_result(attr)

    def __setitem__(self, key, value):
        self.base_object[key] = base_v(value)

    def __or__(self, other):
        return self.wrap_result(self.base_object | base_v(other))

    def __ror__(self, other):
        return self.wrap_result(base_v(other) | self.base_object)

    def __and__(self, other):
        return self.wrap_result(self.base_object & base_v(other))

    def __rand__(self, other):
        return self.wrap_result(base_v(other) & self.base_object)

    def __xor__(self, other):
        return self.wrap_result(self.base_object ^ base_v(other))

    def __rxor__(self, other):
        return self.wrap_result(base_v(other) ^ self.base_object)

    def __invert__(self):
        return self.wrap_result(~ self.base_object)

    def __neg__(self):
        return self.wrap_result(- self.base_object)

    def __gt__(self, other):
        return self.wrap_result(self.base_object > other)

    def __lt__(self, other):
        return self.wrap_result(self.base_object < other)

    def __contains__(self, item):
        return item in self.base_object

    def __iter__(self):
        return self.wrap_result(iter(self.base_object))

    def __bool__(self):
        return len(self.base_object) > 0

    def __len__(self):
        return len(self.base_object)

    def __eq__(self, other):
        return self.wrap_result(self.base_object == other)

    def __ne__(self, other):
        return self.wrap_result(self.base_object != other)

    def wrap_result(self, res):
        return wrap_result(res, self.dataframe_class, self.series_class)

    @property
    def loc(self):
        return self.wrap_result(self.base_object.loc)

    @property
    def iloc(self):
        return self.wrap_result(self.base_object.iloc)


class BaseOasisSeries(ResultWrapper):
    base = pd.Series

    def blank(self):
        s = self.base_object
        return self.wrap_result(s.isin(self.BLANK_VALUES))

    def with_invalid_categories(self, valid_categories):
        s = self.base_object
        split = self.wrap_result(s.astype("str").str.split(';').apply(pd.Series, 1).stack())
        valid = split.isin(set(valid_categories)) | split.blank()
        return self.wrap_result(split[~valid].index.droplevel(-1))


class BaseOasisDataframe(ResultWrapper):
    base = pd.DataFrame

    def blank(self):
        df = self.base_object
        return self.wrap_result(df[df.isin(self.BLANK_VALUES)])


BaseOasisSeries.dataframe_class = BaseOasisDataframe
BaseOasisSeries.series_class = BaseOasisSeries
BaseOasisDataframe.dataframe_class = BaseOasisDataframe
BaseOasisDataframe.series_class = BaseOasisSeries


class DfEngineMeta(type):
    def __getattr__(cls, item):
        res = getattr(cls.module, item)
        return cls.wrap_result(res)


class BaseDfEngine(metaclass=DfEngineMeta):
    module = pd
    DataFrame = BaseOasisDataframe
    Series = BaseOasisSeries

    @classmethod
    def wrap_result(cls, res):
        return wrap_result(res, cls.DataFrame, cls.Series)
