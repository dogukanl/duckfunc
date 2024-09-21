from __future__ import annotations
import re
import base64
import pyarrow as pa
from typing import Any, Self, Literal, Union
from pydantic import Field
from pydantic import BaseModel
from pydantic import ConfigDict
from pydantic import ValidationError
from pydantic import field_serializer
from pydantic import field_validator
from pydantic import model_validator


DUCKDB_MAX_ROWS = 200


class DuckDBQuery(BaseModel):
    model_config = ConfigDict(validate_assignment=True, validate_default=True)

    sql: str
    limit: int | None = None
    offset: int | None = None
    alias: str | None = None
    params: dict[str, Any] | None = None

    def model_post_init(self: Self, _: Any) -> None:
        self.sql = re.sub(r'\s+', ' ', self.sql)
        self.sql = self.sql.strip()

    @field_validator("limit", mode="before")
    def set_limit(cls, limit: int):
        if limit is not None:
            if not isinstance(limit, int):
                raise ValidationError("limit must be of type `int`")
            if limit < 0 or limit > DUCKDB_MAX_ROWS:
                raise ValidationError(
                    f"limit must be between 0 and {DUCKDB_MAX_ROWS}")

            return limit
        return DUCKDB_MAX_ROWS

    @field_validator("offset", mode="before")
    def set_offset(cls, offset: int):
        if offset is not None:
            if not isinstance(offset, int):
                raise ValidationError("offset must be of type `int`")
            if offset < 0:
                raise ValidationError(f"offset must be greater than 0")

            return offset
        return 0


class DuckDBContext(BaseModel):
    database: str = ":memory:"
    readonly: bool = False
    config: dict[str, Any] | None = None


class DuckDBRequest(BaseModel):
    context: DuckDBContext | None = None
    query: DuckDBQuery | None = None


class DuckDBResponse(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)

    success: bool
    compression: Literal["gzip", "bz2", "brotli", "lz4", "zstd"] = "zstd"
    table: pa.Table | str | bytes | None = None
    rowcount: int | None = None
    error: str | None = None
    next: DuckDBRequest | None = None

    def __add__(self: DuckDBResponse, other: DuckDBResponse):
        if self.success != True or other.success != True:
            raise ValueError(
                "both values must have success = True")

        return other.model_copy(update={
            "table": pa.concat_tables([self.table, other.table]),
            "rowcount": self.rowcount + other.rowcount,
        })

    @field_serializer("table", when_used="json")
    def serialize_table(self, table: pa.Table) -> str:

        if table is None:
            return table

        if not isinstance(table, pa.Table):
            raise ValueError(
                "expected `pyarrow.Table` or `None`, "
                f"got: {type(table)}"
            )

        options = pa.ipc.IpcWriteOptions(compression=self.compression)
        ostream = pa.BufferOutputStream()

        with pa.ipc.new_stream(ostream, table.schema, options=options) as writer:
            writer.write(table)

        pybytes = ostream.getvalue().to_pybytes()

        return base64.urlsafe_b64encode(pybytes).decode()

    @model_validator(mode="after")
    def deserialize_table(self) -> Self:

        table = self.table
        if table is None:
            return self

        if isinstance(table, str):
            table = table.encode()
        if isinstance(table, bytes):
            table = base64.urlsafe_b64decode(table)
            istream = pa.BufferReader(table)
            reader = pa.ipc.open_stream(istream)
            table = reader.read_all()

        if not isinstance(table, pa.Table):
            raise ValidationError(
                f"expected `str`, `bytes`, `pyarrow.Table` or `None` but got {type(self.table)}")

        self.table = table
        return self

    @model_validator(mode="after")
    def set_rowcount(self: Self) -> Self:
        if self.table is not None and self.rowcount is None:
            self.rowcount = self.table.num_rows

        return self
