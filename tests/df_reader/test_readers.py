from tempfile import NamedTemporaryFile
from unittest.mock import patch

import numpy as np
import pandas as pd
import pytest

from lot3.df_reader.reader import OasisDaskReader, OasisPandasReader

EXPECTED = {
    "A": {0: 1.0, 1: 1.0, 2: 1.0, 3: 1.0},
    "B": {0: "2023-01-01", 1: "2023-01-02", 2: "2023-01-02", 3: "2023-01-02"},
    "C": {0: 1.0, 1: 1.0, 2: 1.0, 3: 1.0},
    "D": {0: 3, 1: 3, 2: 3, 3: 3},
    "E": {0: "test", 1: "train", 2: "test", 3: "train"},
    "F": {0: "foo", 1: "foo", 2: "foo", 3: "foo"},
}


@pytest.fixture
def df():
    return pd.DataFrame(
        {
            "A": 1.0,
            "B": [
                pd.Timestamp("20230101"),
                pd.Timestamp("20230102"),
                pd.Timestamp("20230102"),
                pd.Timestamp("20230102"),
            ],
            "C": pd.Series(1, index=list(range(4)), dtype="float32"),
            "D": np.array([3] * 4, dtype="int32"),
            "E": pd.Categorical(["test", "train", "test", "train"]),
            "F": "foo",
        }
    )


@pytest.mark.parametrize("reader", [OasisPandasReader, OasisDaskReader])
def test_read_csv__expected_pandas_dataframe(reader, df):
    with NamedTemporaryFile(suffix=".csv") as csv:
        df.to_csv(
            path_or_buf=csv.name, columns=df.columns, encoding="utf-8", index=False
        )

        result = reader().read_csv(csv.name)

        assert isinstance(result, pd.DataFrame)
        assert result.to_dict() == EXPECTED


@pytest.mark.parametrize("reader", [OasisPandasReader, OasisDaskReader])
def test_read_csv__df_filter__expected_pandas_dataframe(reader, df):
    with NamedTemporaryFile(suffix=".csv") as csv:
        df.to_csv(
            path_or_buf=csv.name, columns=df.columns, encoding="utf-8", index=False
        )

        def sample_filter(filter_df):
            return filter_df[filter_df["E"] == "test"]

        result = reader(df_filters=[sample_filter]).read_csv(csv.name)

        assert isinstance(result, pd.DataFrame)
        assert result.to_dict() == {
            "A": {0: 1.0, 2: 1.0},
            "B": {0: "2023-01-01", 2: "2023-01-02"},
            "C": {0: 1.0, 2: 1.0},
            "D": {0: 3, 2: 3},
            "E": {0: "test", 2: "test"},
            "F": {0: "foo", 2: "foo"},
        }


@pytest.mark.parametrize("reader", [OasisPandasReader, OasisDaskReader])
def test_read_csv__df_filter__multiple__expected_pandas_dataframe(reader, df):
    with NamedTemporaryFile(suffix=".csv") as csv:
        df.to_csv(
            path_or_buf=csv.name, columns=df.columns, encoding="utf-8", index=False
        )

        result = reader(
            df_filters=[
                lambda x: x[x["E"] == "test"],
                lambda x: x[x["B"] == "2023-01-02"],
            ]
        ).read_csv(csv.name)

        assert isinstance(result, pd.DataFrame)
        assert result.to_dict() == {
            "A": {2: 1.0},
            "B": {2: "2023-01-02"},
            "C": {2: 1.0},
            "D": {2: 3},
            "E": {2: "test"},
            "F": {2: "foo"},
        }


def test_read_csv__dask__removes_bad_kwargs(df):
    with NamedTemporaryFile(suffix=".csv") as csv:
        df.to_csv(
            path_or_buf=csv.name, columns=df.columns, encoding="utf-8", index=False
        )

        with patch("dask.dataframe.read_csv") as dask_read_csv:
            OasisDaskReader().read_csv(
                csv.name, memory_map=True, low_memory=True, encoding="utf-8"
            )

        assert len(dask_read_csv.call_args[0]) == 1
        assert dask_read_csv.call_args[0][0] == csv.name
        assert len(dask_read_csv.call_args[1]) == 1
        assert dask_read_csv.call_args[1]["encoding"] == "utf-8"


def test_read_csv__dask__sql__expected_pandas_dataframe(df):
    with NamedTemporaryFile(suffix=".csv") as csv:
        df.to_csv(
            path_or_buf=csv.name, columns=df.columns, encoding="utf-8", index=False
        )

        result = OasisDaskReader(
            sql="SELECT A, B, C, D, E, F FROM table WHERE E = 'test' AND B = '2023-01-02'"
        ).read_csv(csv.name)

        assert isinstance(result, pd.DataFrame)
        assert result.to_dict() == {
            "a": {2: 1.0},
            "b": {2: "2023-01-02"},
            "c": {2: 1.0},
            "d": {2: 3},
            "e": {2: "test"},
            "f": {2: "foo"},
        }


def test_read_csv__dask__sql__invalid_sql(df, caplog):
    with NamedTemporaryFile(suffix=".csv") as csv:
        df.to_csv(
            path_or_buf=csv.name, columns=df.columns, encoding="utf-8", index=False
        )

        result = OasisDaskReader(sql="SELECT X FROM table").read_csv(csv.name)

        assert not result
        assert caplog.messages == ["Invalid SQL provided"]


def test_read_csv__dask__sql__no_data(df, caplog):
    with NamedTemporaryFile(suffix=".csv") as csv:
        df.to_csv(
            path_or_buf=csv.name, columns=df.columns, encoding="utf-8", index=False
        )

        result = OasisDaskReader(
            sql="SELECT A, B, C, D, E, F FROM table WHERE E = 'tester'"
        ).read_csv(csv.name)

        assert isinstance(result, pd.DataFrame)
        assert result.to_dict() == {
            "a": {},
            "b": {},
            "c": {},
            "d": {},
            "e": {},
            "f": {},
        }
