import operator

import numpy as np
import pytest

from lot3.df_engine.base import wrap_result, BaseOasisDataframe, BaseOasisSeries


class WrapableObject:
    def __init__(self):
        self.base = {"value": 100, "str": "foo"}
        self.other_prop = "test"

    def __getitem__(self, item):
        return self.base[item]

    def __setitem__(self, item, value):
        return self.base[item]

    def __delitem__(self, item):
        del self.base[item]
        return self

    def __or__(self, other):
        return {"value": self.base["value"] | other}

    def __ror__(self, other):
        return {"value": other | self.base["value"]}

    def __and__(self, other):
        return {"value": self.base["value"] & other}

    def __rand__(self, other):
        return {"value": other & self.base["value"]}

    def __xor__(self, other):
        return {"value": self.base["value"] ^ other}

    def __rxor__(self, other):
        return {"value": other ^ self.base["value"]}

    def __invert__(self):
        return {"value": ~self.base["value"]}

    def __add__(self, other):
        return {"value": self.base["value"] + other}

    def __radd__(self, other):
        return {"value": other + self.base["value"]}

    def __sub__(self, other):
        return {"value": self.base["value"] - other}

    def __rsub__(self, other):
        return {"value": other - self.base["value"]}

    def __mul__(self, other):
        return {"value": self.base["value"] * other}

    def __rmul__(self, other):
        return {"value": other * self.base["value"]}

    def __matmul__(self, other):
        return {"value": self.base["value"] * other}

    def __rmatmul__(self, other):
        return {"value": other * self.base["value"]}

    def __pow__(self, other):
        return {"value": self.base["value"] ** other}

    def __rpow__(self, other):
        return {"value": other ** self.base["value"]}

    def __truediv__(self, other):
        return {"value": self.base["value"] / other}

    def __rtruediv__(self, other):
        return {"value": other / self.base["value"]}

    def __floordiv__(self, other):
        return {"value": self.base["value"] // other}

    def __rfloordiv__(self, other):
        return {"value": other // self.base["value"]}

    def __mod__(self, other):
        return {"value": self.base["value"] % other}

    def __rmod__(self, other):
        return {"value": other % self.base["value"]}

    def __neg__(self):
        return {"value": -self.base["value"]}

    def __gt__(self, other):
        return self.base["value"] > other

    def __lt__(self, other):
        return self.base["value"] < other

    def __ge__(self, other):
        return self.base["value"] >= other

    def __le__(self, other):
        return self.base["value"] <= other

    def __contains__(self, item):
        return item in self.base

    def __iter__(self):
        return iter(self.base)

    def __bool__(self):
        return bool(self.base["value"])

    def __len__(self):
        return len(self.base)

    def __eq__(self, other):
        return self.base == other

    def __req__(self, other):
        return other == self.base

    def __ne__(self, other):
        return self.base["value"] != other

    def __rne__(self, other):
        return other != self.base["value"]

    def __call__(self):
        return self.base

    def __hash__(self):
        return hash(self.base)

    def __array__(self):
        return np.array(self.base)

    def __rshift__(self, other):
        return {"value": self.base["value"] >> other}

    def __rrshift__(self, other):
        return {"value": other >> self.base["value"]}

    def __lshift__(self, other):
        return {"value": self.base["value"] << other}

    def __rlshift__(self, other):
        return {"value": other << self.base["value"]}

    def __del__(self):
        del self.base["value"]


lr_operators = [
    operator.__eq__,
    operator.__ne__,
    operator.__add__,
    operator.__and__,
    operator.__floordiv__,
    operator.__mod__,
    operator.__mul__,
    operator.__matmul__,
    operator.__or__,
    operator.__pow__,
    operator.__sub__,
    operator.__truediv__,
    operator.__xor__,
]

l_operators = [
    operator.__lt__,
    operator.__le__,
    operator.__eq__,
    operator.__ne__,
    operator.__ge__,
    operator.__gt__,
    operator.__iadd__,
    operator.__iand__,
    operator.__ifloordiv__,
    operator.__ilshift__,
    operator.__imod__,
    operator.__imul__,
    operator.__imatmul__,
    operator.__ior__,
    operator.__ipow__,
    operator.__irshift__,
    operator.__isub__,
    operator.__itruediv__,
    operator.__ixor__,
    operator.__contains__,
    operator.__rshift__,
    operator.__lshift__,
]


def __del__(x):
    del x
    return x


unary_operators = [
    operator.__not__,
    operator.__invert__,
    operator.__neg__,
    operator.__pos__,
    operator.__abs__,
    iter,
    bool,
    len,
    hash,
    operator.__call__,
    np.array,
    __del__,
    operator.__index__,
]


@pytest.mark.parametrize("value", [
    int(1), float(1.1), "str", b"bytes", True, False, None, np.int32(1),
    np.int64(1), np.ndarray((2, 2)), np.dtype("int"), BaseOasisDataframe.base_index([]),
])
def test_value_is_literal___object_isnt_wrapped(value):
    wrapped = wrap_result(value, BaseOasisDataframe, BaseOasisSeries)

    assert type(wrapped) == type(value)


def test_value_is_dataframe___object_wrapped_dataframe_class():
    wrapped = wrap_result(BaseOasisDataframe.base([[1, 2], [3, 4]]), BaseOasisDataframe, BaseOasisSeries)

    assert type(wrapped) == BaseOasisDataframe


def test_value_is_series___object_wrapped_series_class():
    wrapped = wrap_result(BaseOasisSeries.base([1, 2, 3, 4]), BaseOasisDataframe, BaseOasisSeries)

    assert type(wrapped) == BaseOasisSeries


@pytest.mark.parametrize("operator", lr_operators + l_operators)
def test_left_operators___result_is_the_same_as_the_base(operator):
    wrapped = wrap_result(WrapableObject(), BaseOasisDataframe, BaseOasisSeries)
    unwrapped = WrapableObject()

    assert operator(wrapped, 2) == operator(unwrapped, 2)


@pytest.mark.parametrize("operator", lr_operators)
def test_right_operators___result_is_the_same_as_the_base(operator):
    wrapped = wrap_result(WrapableObject(), BaseOasisDataframe, BaseOasisSeries)
    unwrapped = WrapableObject()

    assert operator(2, wrapped) == operator(2, unwrapped)


def test_getitem():
    wrapped = wrap_result(WrapableObject(), BaseOasisDataframe, BaseOasisSeries)
    unwrapped = WrapableObject()

    assert wrapped["value"] == unwrapped["value"]


def test_setitem():
    wrapped = wrap_result(WrapableObject(), BaseOasisDataframe, BaseOasisSeries)
    unwrapped = WrapableObject()

    wrapped["value"] = "foo"
    unwrapped["value"] = "foo"

    assert wrapped == wrapped


def test_delitem():
    wrapped = wrap_result(WrapableObject(), BaseOasisDataframe, BaseOasisSeries)
    unwrapped = WrapableObject()

    del wrapped["value"]
    del unwrapped["value"]

    assert wrapped == wrapped


def test_getattr():
    wrapped = wrap_result(WrapableObject(), BaseOasisDataframe, BaseOasisSeries)
    unwrapped = WrapableObject()

    return wrapped.other_prop == unwrapped.other_prop


def test_setattr():
    wrapped = wrap_result(WrapableObject(), BaseOasisDataframe, BaseOasisSeries)

    wrapped.other_prop = "updated"

    return wrapped.base_object.other_prop == "updated"


