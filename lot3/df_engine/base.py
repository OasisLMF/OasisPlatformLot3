from typing import Callable, Iterable

import numpy as np
import pandas as pd


def base_v(v):
    if isinstance(v, (list, set, tuple, )):
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

def _getitem(self, item):
    attr = self.base_object[base_v(item)]
    return self.wrap_result(attr)


def _setitem(self, key, value):
    if isinstance(key, tuple):
        key = tuple(base_v(k) for k in key)
    else:
        key = base_v(key)

    self.base_object[key] = base_v(value)


def _or(self, other):
    return self.wrap_result(self.base_object | base_v(other))


def _ror(self, other):
    return self.wrap_result(base_v(other) | self.base_object)


def _and(self, other):
    return self.wrap_result(self.base_object & base_v(other))


def _rand(self, other):
    return self.wrap_result(base_v(other) & self.base_object)


def _xor(self, other):
    return self.wrap_result(self.base_object ^ base_v(other))


def _rxor(self, other):
    return self.wrap_result(base_v(other) ^ self.base_object)


def _invert(self):
    return self.wrap_result(~ self.base_object)


def _add(self, other):
    return self.wrap_result(self.base_object + base_v(other))


def _radd(self, other):
    return self.wrap_result(base_v(other) + self.base_object)


def _sub(self, other):
    return self.wrap_result(self.base_object + base_v(other))


def _rsub(self, other):
    return self.wrap_result(base_v(other) + self.base_object)


def _mul(self, other):
    return self.wrap_result(self.base_object * base_v(other))


def _rmul(self, other):
    return self.wrap_result(base_v(other) * self.base_object)


def _truediv(self, other):
    return self.wrap_result(self.base_object / base_v(other))


def _rtruediv(self, other):
    return self.wrap_result(base_v(other) / self.base_object)


def _floordiv(self, other):
    return self.wrap_result(self.base_object // base_v(other))


def _rfloordiv(self, other):
    return self.wrap_result(base_v(other) // self.base_object)


def _mod(self, other):
    return self.wrap_result(self.base_object % base_v(other))


def _rmod(self, other):
    return self.wrap_result(self.base_object % base_v(other))


def _neg(self):
    return self.wrap_result(- self.base_object)


def _gt(self, other):
    return self.wrap_result(self.base_object > base_v(other))


def _lt(self, other):
    return self.wrap_result(self.base_object < base_v(other))


def _contains(self, item):
    return base_v(item) in self.base_object


def _iter(self):
    return (self.wrap_result(i) for i in self.base_object)


def _bool(self):
    return bool(self.base_object)


def _len(self):
    return len(self.base_object)


def _eq(self, other):
    return self.wrap_result(self.base_object == base_v(other))


def _req(self, other):
    return self.wrap_result(base_v(other) == self.base_object)


def _ne(self, other):
    return self.wrap_result(self.base_object != base_v(other))


def _rne(self, other):
    return self.wrap_result(base_v(other) != self.base_object)


def _call(self, *args, **kwargs):
    processed_args = base_v(args)
    processed_kwargs = base_v(kwargs)

    return self.wrap_result(
        self.base_object(*processed_args, **processed_kwargs),
    )


def _array(self):
    return self.base_object.__array__()


def _hash(self):
    return hash(self.base_object)


def _rshift(self, other):
    return self.wrap_result(self.base_object >> base_v(other))


def _rrshift(self, other):
    return self.wrap_result(base_v(other) >> self.base_object)


def _lshift(self, other):
    return self.wrap_result(self.base_object << base_v(other))


def _rlshift(self, other):
    return self.wrap_result(base_v(other) << self.base_object)


overrides = {
    "__getitem__": _getitem,
    "__setitem__": _setitem,
    "__or__": _or,
    "__ror__": _ror,
    "__and__": _and,
    "__rand__": _rand,
    "__xor__": _xor,
    "__rxor__": _rxor,
    "__invert__": _invert,
    "__add__": _add,
    "__radd__": _radd,
    "__sub__": _sub,
    "__rsub__": _rsub,
    "__mul__": _mul,
    "__rmul__": _rmul,
    "__truediv__": _truediv,
    "__rtruediv__": _rtruediv,
    "__floordiv__": _floordiv,
    "__rfloordiv__": _rfloordiv,
    "__mod__": _mod,
    "__rmod__": _rmod,
    "__neg__": _neg,
    "__gt__": _gt,
    "__lt__": _lt,
    "__contains__": _contains,
    "__iter__": _iter,
    "__bool__": _bool,
    "__len__": _len,
    "__eq__": _eq,
    "__req__": _req,
    "__ne__": _ne,
    "__rne__": _rne,
    "__call__": _call,
    "__hash__": _hash,
    "__array__": _array,
    "__rshift__": _rshift,
    "__rrshift__": _rrshift,
    "__lshift__": _lshift,
    "__rlshift__": _rlshift,
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
    if isinstance(res, WrappedBase):
        # ensure we dont wrap a wrapped object
        return res

    if isinstance(res, (int, float, str, bytes, bool, np.int, np.float, np.str, np.bool, np.long, np.int64, np.ndarray)):
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
            k: overrides[k]
            for k in dir(res) if k in overrides
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

    __getitem__ = _getitem
    __setitem__ = _setitem
    __or__ = _or
    __ror__ = _ror
    __and__ = _and
    __rand__ = _rand
    __xor__ = _xor
    __rxor__ = _rxor
    __invert__ = _invert
    __add__ = _add
    __radd__ = _radd
    __sub__ = _sub
    __rsub__ = _rsub
    __mul__ = _mul
    __rmul__ = _rmul
    __truediv__ = _truediv
    __rtruediv__ = _rtruediv
    __floordiv__ = _floordiv
    __rfloordiv__ = _rfloordiv
    __mod__ = _mod
    __rmod__ = _rmod
    __neg__ = _neg
    __gt__ = _gt
    __lt__ = _lt
    __contains__ = _contains
    __iter__ = _iter
    __bool__ = _bool
    __len__ = _len
    __eq__ = _eq
    __ne__ = _ne
    __array__: _array
    __rshift__: _rshift
    __rrshift__: _rrshift
    __lshift__: _lshift
    __rlshift__: _rlshift


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
