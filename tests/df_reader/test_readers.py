from tempfile import NamedTemporaryFile
from unittest.mock import patch

import numpy as np
import pandas as pd
import pytest

from lot3.df_reader.reader import (OasisDaskReader, OasisPandasReader,
                                   OasisSparkReader)


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
            "C": pd.Series(1, index=list(range(4)), dtype="float64"),
            "D": np.array([3] * 4),
            "E": pd.Categorical(["test", "train", "test", "train"]),
            "F": "foo",
        }
    )


@pytest.mark.parametrize(
    "reader", [OasisPandasReader, OasisDaskReader, OasisSparkReader]
)
def test_read_csv__expected_pandas_dataframe(reader, df):
    with NamedTemporaryFile(suffix=".csv") as csv:
        df.to_csv(
            path_or_buf=csv.name, columns=df.columns, encoding="utf-8", index=False
        )

        result = reader().read_csv(csv.name)

        assert isinstance(result, pd.DataFrame)
        assert result.to_dict() == {
            "A": {0: 1.0, 1: 1.0, 2: 1.0, 3: 1.0},
            "B": {
                0: "2023-01-01",
                1: "2023-01-02",
                2: "2023-01-02",
                3: "2023-01-02",
            },
            "C": {0: 1.0, 1: 1.0, 2: 1.0, 3: 1.0},
            "D": {0: 3, 1: 3, 2: 3, 3: 3},
            "E": {0: "test", 1: "train", 2: "test", 3: "train"},
            "F": {0: "foo", 1: "foo", 2: "foo", 3: "foo"},
        }


@pytest.mark.parametrize(
    "reader", [OasisPandasReader, OasisDaskReader, OasisSparkReader]
)
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


@pytest.mark.parametrize(
    "reader", [OasisPandasReader, OasisDaskReader, OasisSparkReader]
)
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
            sql="SELECT * FROM table WHERE E = 'test' AND B = '2023-01-02'"
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

        result = OasisDaskReader(sql="SELECT * FROM table WHERE E = 'tester'").read_csv(
            csv.name
        )

        assert isinstance(result, pd.DataFrame)
        assert result.to_dict() == {
            "A": {},
            "B": {},
            "C": {},
            "D": {},
            "E": {},
            "F": {},
        }


def test_read_csv__spark__sql__expected_pandas_dataframe(df):
    with NamedTemporaryFile(suffix=".csv") as csv:
        df.to_csv(
            path_or_buf=csv.name, columns=df.columns, encoding="utf-8", index=False
        )

        result = OasisSparkReader(
            sql="SELECT * FROM table WHERE E = 'test' AND B = '2023-01-02'"
        ).read_csv(csv.name)

        assert isinstance(result, pd.DataFrame)

        # spark has no concept of indexing, so it will essentially drop it hence, 2 being 0 below
        # if we do need to retain indexing we will need to add it as column prior to sql
        assert result.to_dict() == {
            "A": {0: 1.0},
            "B": {0: "2023-01-02"},
            "C": {0: 1.0},
            "D": {0: 3},
            "E": {0: "test"},
            "F": {0: "foo"},
        }


def test_read_csv__spark__sql__invalid_sql(df, caplog):
    with NamedTemporaryFile(suffix=".csv") as csv:
        df.to_csv(
            path_or_buf=csv.name, columns=df.columns, encoding="utf-8", index=False
        )

        result = OasisSparkReader(sql="SELECT X FROM table").read_csv(csv.name)

        assert not result
        assert caplog.messages == ["Invalid SQL provided"]


def test_read_csv__spark__sql__no_data(df, caplog):
    with NamedTemporaryFile(suffix=".csv") as csv:
        df.to_csv(
            path_or_buf=csv.name, columns=df.columns, encoding="utf-8", index=False
        )

        result = OasisSparkReader(
            sql="SELECT * FROM table WHERE E = 'tester'"
        ).read_csv(csv.name)

        assert isinstance(result, pd.DataFrame)
        assert result.to_dict() == {
            "A": {},
            "B": {},
            "C": {},
            "D": {},
            "E": {},
            "F": {},
        }
