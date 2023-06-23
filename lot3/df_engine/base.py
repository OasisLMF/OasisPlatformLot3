import operator
from types import NoneType

import numpy as np
import pandas as pd


def base_v(v):
    if isinstance(v, tuple) and hasattr(v, "_fields"):
        return type(v)(*[base_v(i) for i in v])
    elif isinstance(v, (list, set, tuple, )):
        return type(v)(base_v(i) for i in v)
    elif isinstance(v, dict):
        return {base_v(k): base_v(_v) for k, _v in v.items()}
    return getattr(v, "base_object", v)


class WrappedMeta(type):
    def __getattr__(cls, item):
        res = getattr(cls.base, item)
        return wrap_result(res, cls.dataframe_class, cls.series_class)

###
# the set of override functions to use in wrapped classes
# we cant just assign all to each override class as this
# can break check. For example, if we provide a __call__
# override for each wrapped object we would break all
# all checks for "callable"
###


def operation_caller(operation):
    def op(self, *args, **kwargs):
        return self.wrap_result(operation(self.base_object, *base_v(args), **base_v(kwargs)))

    return op


def rev_operation_caller(operation):
    def op(self, *args, **kwargs):
        return self.wrap_result(operation(*base_v(args), self.base_object, **base_v(kwargs)))

    return op


def __del__(o):
    del o


operators = {
    "__add__": operation_caller(operator.__add__),
    "__and__": operation_caller(operator.__and__),
    "__floordiv__": operation_caller(operator.__floordiv__),
    "__index__": operation_caller(operator.__index__),
    "__lshift__": operation_caller(operator.__lshift__),
    "__mod__": operation_caller(operator.__mod__),
    "__mul__": operation_caller(operator.__mul__),
    "__matmul__": operation_caller(operator.__matmul__),
    "__or__": operation_caller(operator.__or__),
    "__pow__": operation_caller(operator.__pow__),
    "__rshift__": operation_caller(operator.__rshift__),
    "__sub__": operation_caller(operator.__sub__),
    "__truediv__": operation_caller(operator.__truediv__),
    "__xor__": operation_caller(operator.__xor__),
    "__contains__": operation_caller(operator.__contains__),
    "__delitem__": operation_caller(operator.__delitem__),
    "__getitem__": operation_caller(operator.__getitem__),
    "__setitem__": operation_caller(operator.__setitem__),
    "__lt__": operation_caller(operator.__lt__),
    "__le__": operation_caller(operator.__le__),
    "__eq__": operation_caller(operator.__eq__),
    "__ne__": operation_caller(operator.__ne__),
    "__ge__": operation_caller(operator.__ge__),
    "__gt__": operation_caller(operator.__gt__),
    "__iadd__": operation_caller(operator.__iadd__),
    "__iand__": operation_caller(operator.__iand__),
    "__ifloordiv__": operation_caller(operator.__ifloordiv__),
    "__ilshift__": operation_caller(operator.__ilshift__),
    "__imod__": operation_caller(operator.__imod__),
    "__imul__": operation_caller(operator.__imul__),
    "__imatmul__": operation_caller(operator.__imatmul__),
    "__ior__": operation_caller(operator.__ior__),
    "__ipow__": operation_caller(operator.__ipow__),
    "__irshift__": operation_caller(operator.__irshift__),
    "__isub__": operation_caller(operator.__isub__),
    "__itruediv__": operation_caller(operator.__itruediv__),
    "__ixor__": operation_caller(operator.__ixor__),
    "__not__": operation_caller(operator.__not__),
    "__invert__": operation_caller(operator.__invert__),
    "__neg__": operation_caller(operator.__neg__),
    "__pos__": operation_caller(operator.__pos__),
    "__abs__": operation_caller(operator.__abs__),
    "__iter__": lambda self: (self.wrap_result(i) for i in self.base_object),
    "__bool__": operation_caller(bool),
    "__len__": operation_caller(len),
    "__hash__": operation_caller(hash),
    "__call__": operation_caller(operator.__call__),
    "__array__": operation_caller(np.array),
    "__del__": operation_caller(__del__),
    "__ror__": rev_operation_caller(operator.__or__),
    "__rand__": rev_operation_caller(operator.__and__),
    "__rxor__": rev_operation_caller(operator.__xor__),
    "__radd__": rev_operation_caller(operator.__add__),
    "__rsub__": rev_operation_caller(operator.__sub__),
    "__rmul__": rev_operation_caller(operator.__mul__),
    "__rmatmul__": rev_operation_caller(operator.__matmul__),
    "__rtruediv__": rev_operation_caller(operator.__truediv__),
    "__rfloordiv__": rev_operation_caller(operator.__floordiv__),
    "__rmod__": rev_operation_caller(operator.__mod__),
    "__rpow__": rev_operation_caller(operator.__pow__),
}


class WrappedBase(metaclass=WrappedMeta):
    def __init__(self, base_object, dataframe_class, series_class):
        self.base_object = base_object
        self.dataframe_class = dataframe_class
        self.series_class = series_class

    def __getattr__(self, item):
        attr = getattr(self.base_object, item)
        return self.wrap_result(attr)

    def __setattr__(self, key, value):
        if key in ["base_object", "dataframe_class", "series_class"]:
            super().__setattr__(key, value)
        else:
            setattr(self.base_object, key, base_v(value))

    def wrap_result(self, res):
        return wrap_result(res, self.dataframe_class, self.series_class)


def wrap_result(res, dataframe_class, series_class):
    if isinstance(res, type):
        def wrapped_init(*args, **kwargs):
            return wrap_result(res(*args, **kwargs), dataframe_class, series_class)
        return wrapped_init

    if isinstance(res, WrappedBase):
        # ensure we dont wrap a wrapped object
        return res

    ignored_types = (
        int,
        float,
        str,
        bytes,
        bool,
        NoneType,
        np.int32,
        np.int64,
        np.ndarray,
        np.dtype,
        dataframe_class.base_index,
    )
    if isinstance(res, ignored_types):
        return res
    if isinstance(res, dataframe_class.base):
        return dataframe_class(res)
    elif isinstance(res, series_class.base):
        return series_class(res)

    # build a class to wrap all results.
    _wrapper = type(
        f"{type(res).__name__}Wrapper",
        (WrappedBase, ),
        {
            k: operators[k]
            for k in dir(res) if k in operators
        }
    )
    return _wrapper(res, dataframe_class, series_class)


class ResultWrapper(WrappedBase):
    BLANK_VALUES = {np.nan, '', None, pd.NA, pd.NaT}

    def __init__(self, data=None, **kwargs):
        data = base_v(data)
        processed_kwargs = {k: base_v(v) for k, v in kwargs.items()}
        super().__init__(
            data if isinstance(data, self.base) else self.base(data=data, **processed_kwargs),
            self.dataframe_class,
            self.series_class,
        )

    __add__ = operators["__add__"]
    __and__ = operators["__and__"]
    __floordiv__ = operators["__floordiv__"]
    __index__ = operators["__index__"]
    __lshift__ = operators["__lshift__"]
    __mod__ = operators["__mod__"]
    __mul__ = operators["__mul__"]
    __matmul__ = operators["__matmul__"]
    __or__ = operators["__or__"]
    __pow__ = operators["__pow__"]
    __rshift__ = operators["__rshift__"]
    __sub__ = operators["__sub__"]
    __truediv__ = operators["__truediv__"]
    __xor__ = operators["__xor__"]
    __contains__ = operators["__contains__"]
    __delitem__ = operators["__delitem__"]
    __getitem__ = operators["__getitem__"]
    __setitem__ = operators["__setitem__"]
    __lt__ = operators["__lt__"]
    __le__ = operators["__le__"]
    __eq__ = operators["__eq__"]
    __ne__ = operators["__ne__"]
    __ge__ = operators["__ge__"]
    __gt__ = operators["__gt__"]
    __iadd__ = operators["__iadd__"]
    __iand__ = operators["__iand__"]
    __ifloordiv__ = operators["__ifloordiv__"]
    __ilshift__ = operators["__ilshift__"]
    __imod__ = operators["__imod__"]
    __imul__ = operators["__imul__"]
    __imatmul__ = operators["__imatmul__"]
    __ior__ = operators["__ior__"]
    __ipow__ = operators["__ipow__"]
    __irshift__ = operators["__irshift__"]
    __isub__ = operators["__isub__"]
    __itruediv__ = operators["__itruediv__"]
    __ixor__ = operators["__ixor__"]
    __invert__ = operators["__invert__"]
    __neg__ = operators["__neg__"]
    __pos__ = operators["__pos__"]
    __abs__ = operators["__abs__"]
    __iter__ = operators["__iter__"]
    __bool__ = operators["__bool__"]
    __len__ = operators["__len__"]
    __hash__ = operators["__hash__"]
    __call__ = operators["__call__"]
    __array__ = operators["__array__"]
    __del__ = operators["__del__"]
    __ror__ = operators["__ror__"]
    __rand__ = operators["__rand__"]
    __rxor__ = operators["__rxor__"]
    __radd__ = operators["__radd__"]
    __rsub__ = operators["__rsub__"]
    __rmul__ = operators["__rmul__"]
    __rmatmul__ = operators["__rmatmul__"]
    __rtruediv__ = operators["__rtruediv__"]
    __rfloordiv__ = operators["__rfloordiv__"]
    __rmod__ = operators["__rmod__"]
    __rpow__ = operators["__rpow__"]


class BaseOasisSeries(ResultWrapper):
    base = pd.Series
    base_index = pd.Index

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
    base_index = pd.Index

    def blank(self):
        df = self.base_object
        return self.wrap_result(df[df.isin(self.BLANK_VALUES)])


BaseOasisSeries.dataframe_class = BaseOasisDataframe
BaseOasisSeries.series_class = BaseOasisSeries
BaseOasisDataframe.dataframe_class = BaseOasisDataframe
BaseOasisDataframe.series_class = BaseOasisSeries


class BaseDfEngine:
    module = pd
    DataFrame = BaseOasisDataframe
    Series = BaseOasisSeries

    def __getattr__(self, item):
        res = getattr(self.module, item)
        return self.wrap_result(res)

    def wrap_result(self, res):
        return wrap_result(res, self.DataFrame, self.Series)
