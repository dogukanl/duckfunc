import pytest
import duckdb
import pyarrow as pa
import pyarrow.compute as pc
from typing import Any
from collections.abc import Iterable
from duckfunc import (
    handle,
    DuckDBContext,
    DuckDBQuery,
    DuckDBRequest,
    DuckDBResponse,
    DUCKDB_MAX_ROWS,
)


TEST_HANDLE_CASES = [
    [
        DuckDBRequest(context=None, query=DuckDBQuery(sql="")),
        DuckDBResponse(success=True),
    ],
    [
        DuckDBRequest(
            context=None,
            query=DuckDBQuery(
                sql="SELECT i FROM generate_series(1, 3) t(i)"
            )),
        DuckDBResponse(
            success=True,
            rowcount=3,
            table=pa.table([pa.array([1, 2, 3])], names=["i"]),
        )
    ],
    [
        DuckDBRequest(
            context=None,
            query=DuckDBQuery(
                sql="SELECT i FROM generate_series(1, 200) t(i)",
            )),
        DuckDBResponse(
            success=True,
            rowcount=200,
            table=pa.table([pa.array(range(1, 201))], names=["i"]),
            next=DuckDBRequest(
                context=None,
                query=DuckDBQuery(
                    sql="SELECT i FROM generate_series(1, 200) t(i)",
                    limit=DUCKDB_MAX_ROWS,
                    offset=200,
                ))
        )
    ]
]


@pytest.mark.parametrize(
    argnames="input, expected",
    argvalues=TEST_HANDLE_CASES,
    ids=["empty_query", "simple_query", "paged_query"],
)
def test_handle(input, expected):
    assert handle(input) == expected
