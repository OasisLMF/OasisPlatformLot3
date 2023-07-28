from tempfile import NamedTemporaryFile

import numpy as np
import pandas as pd
import pytest

from lot3.df_reader.exceptions import InvalidSQLException
from lot3.df_reader.reader import OasisDaskReaderCSV

# Test readers in more detail, base SQL is tested for both CSV and parquet separate to this.


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
                pd.Timestamp("20230101"),
                pd.Timestamp("20230103"),
            ],
            "C": pd.Series(1, index=list(range(6)), dtype="float64"),
            "D": np.array([3] * 6),
            "E": pd.Categorical(["test", "train", "test", "train", "else", "other"]),
            "F": "foo",
        }
    )


def _test_sql(df, sql):
    with NamedTemporaryFile(suffix=".csv") as csv:
        df.to_csv(
            path_or_buf=csv.name, columns=df.columns, encoding="utf-8", index=False
        )

        result = OasisDaskReaderCSV(csv.name).sql(sql).as_pandas()
        assert isinstance(result, pd.DataFrame)
        return result


@pytest.mark.parametrize(
    "sql",
    (
        "SELECT * FROM table",
        "SELECT * FROM table WHERE E = 1",
        "SELECT * FROM table WHERE E = 1 and A = 2",
        "SELECT * FROM table WHERE A in (1,2)",
        "SELECT C FROM table WHERE C BETWEEN 1 AND 2",
        "SELECT * FROM table ORDER BY A",
    ),
)
def test_sql__validity(sql, df):
    result = _test_sql(df, sql)
    assert isinstance(result, pd.DataFrame)


@pytest.mark.parametrize(
    "sql",
    (
        "SELECT * FROM s",  # incorrect table
        "SELECT Z FROM table",  # incorrect field
        "X",
        "SELECT SUM(Z) FROM table",  # incorrect field
    ),
)
def test_sql__validity__not(sql, df):
    with pytest.raises(InvalidSQLException):
        _test_sql(df, sql)


def test_sql__where(df):
    result = _test_sql(df, "SELECT * FROM table WHERE E = 'test'")
    assert isinstance(result, pd.DataFrame)
    assert result.to_dict() == {
        "A": {0: 1.0, 2: 1.0},
        "B": {0: "2023-01-01", 2: "2023-01-02"},
        "C": {0: 1.0, 2: 1.0},
        "D": {0: 3, 2: 3},
        "E": {0: "test", 2: "test"},
        "F": {0: "foo", 2: "foo"},
    }


def test_sql__where_like(df):
    result = _test_sql(df, "SELECT * FROM table WHERE E LIKE 'te%'")
    assert isinstance(result, pd.DataFrame)
    assert result.to_dict() == {
        "A": {0: 1.0, 2: 1.0},
        "B": {0: "2023-01-01", 2: "2023-01-02"},
        "C": {0: 1.0, 2: 1.0},
        "D": {0: 3, 2: 3},
        "E": {0: "test", 2: "test"},
        "F": {0: "foo", 2: "foo"},
    }


def test_sql__where_null():
    df = pd.DataFrame(
        {
            "A": [1, None],
            "B": ["foo", "bar"],
        }
    )
    result = _test_sql(df, "SELECT * FROM table WHERE A IS NULL = FALSE")
    assert isinstance(result, pd.DataFrame)
    assert result.to_dict() == {"A": {0: 1.0}, "B": {0: "foo"}}


def test_sql__where_in(df):
    result = _test_sql(df, "SELECT C FROM table WHERE C in (1,2, 3)")
    assert isinstance(result, pd.DataFrame)
    assert result.to_dict() == {"C": {0: 1.0, 1: 1.0, 2: 1.0, 3: 1.0, 4: 1.0, 5: 1.0}}


def test_sql__where_between(df):
    result = _test_sql(df, "SELECT C FROM table WHERE C BETWEEN 1 AND 3")
    assert isinstance(result, pd.DataFrame)
    assert result.to_dict() == {"C": {0: 1.0, 1: 1.0, 2: 1.0, 3: 1.0, 4: 1.0, 5: 1.0}}


def test_sql__where_and(df):
    result = _test_sql(df, "SELECT * FROM table WHERE E = 'test' AND B = '2023-01-02'")
    assert isinstance(result, pd.DataFrame)
    assert result.to_dict() == {
        "A": {2: 1.0},
        "B": {2: "2023-01-02"},
        "C": {2: 1.0},
        "D": {2: 3},
        "E": {2: "test"},
        "F": {2: "foo"},
    }


def test_sql__order_by__asc(df):
    result = _test_sql(df, "SELECT * FROM table ORDER BY E")
    assert result.iloc[0].to_dict() == {
        "A": 1.0,
        "B": "2023-01-01",
        "C": 1.0,
        "D": 3,
        "E": "else",
        "F": "foo",
    }


def test_sql__order_by__desc(df):
    result = _test_sql(df, "SELECT * FROM table ORDER BY E DESC")
    assert result.iloc[0].to_dict() == {
        "A": 1.0,
        "B": "2023-01-02",
        "C": 1.0,
        "D": 3,
        "E": "train",
        "F": "foo",
    }


def test_sql__order_by__multiple(df):
    result = _test_sql(df, "SELECT * FROM table ORDER BY E, D DESC")
    assert result.iloc[0].to_dict() == {
        "A": 1.0,
        "B": "2023-01-01",
        "C": 1.0,
        "D": 3,
        "E": "else",
        "F": "foo",
    }
    assert result.iloc[1].to_dict() == {
        "A": 1.0,
        "B": "2023-01-03",
        "C": 1.0,
        "D": 3,
        "E": "other",
        "F": "foo",
    }


def test_sql__alias(df):
    result = _test_sql(df, 'SELECT A as "else"  FROM table')
    assert result.to_dict() == {
        "else": {0: 1.0, 1: 1.0, 2: 1.0, 3: 1.0, 4: 1.0, 5: 1.0}
    }


def test_sql__cast(df):
    result = _test_sql(df, 'SELECT UPPER(E) as "upper_E" FROM table')
    assert result.to_dict() == {
        "upper_E": {0: "TEST", 1: "TRAIN", 2: "TEST", 3: "TRAIN", 4: "ELSE", 5: "OTHER"}
    }


def test_sql__aggregation__count(df):
    result = _test_sql(df, 'SELECT COUNT(A) as "count_A" FROM table')
    assert result.to_dict() == {"count_A": {0: 6}}


def test_sql__aggregation__count_group(df):
    result = _test_sql(df, 'SELECT COUNT(A) as "count_A" FROM table GROUP BY E')
    assert result.to_dict() == {"count_A": {0: 2, 1: 2, 2: 1, 3: 1}}


def test_sql__aggregation__sum(df):
    result = _test_sql(df, 'SELECT E, SUM(A) AS "sum_A" FROM table GROUP BY E')
    assert result.to_dict() == {
        "E": {0: "test", 1: "train", 2: "else", 3: "other"},
        "sum_A": {0: 2.0, 1: 2.0, 2: 1.0, 3: 1.0},
    }
