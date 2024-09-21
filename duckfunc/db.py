import base64
import duckdb
import threading
import duckdb.typing
import pyarrow as pa
import structlog
from .models import (
    DuckDBContext,
    DuckDBRequest,
    DuckDBResponse,
)

conn: duckdb.DuckDBPyConnection = None
logger = structlog.get_logger()


def init(context: DuckDBContext = None) -> None:
    global conn

    if conn is None:

        if context is not None:
            context = context.model_dump()
        else:
            context = {}

        logger.info(f"initializing duckdb connection using {context=}")
        conn = duckdb.connect(**context)


def handle(r: DuckDBRequest) -> DuckDBResponse:

    init(r.context)

    with conn.cursor() as cursor:

        try:
            logger.debug(f"handling: {r}")
            rel = cursor.sql(query=r.query.sql, params=r.query.params)

            # empty query
            if rel is None:
                logger.debug(
                    f"query `{r.query.sql}` returned an empty relation")
                return DuckDBResponse(success=True)

            rel = rel.limit(r.query.limit, offset=r.query.offset)

            table = rel.arrow()
            rowcount = table.num_rows

            logger.debug(f"fetched {rowcount} rows")

            next_ = None
            if rowcount == r.query.limit:
                offset = r.query.limit + r.query.offset

                r.query = r.query.model_copy(update={"offset": offset})
                next_ = r.model_copy(update={"query": r.query})

            return DuckDBResponse(
                success=True,
                rowcount=rowcount,
                table=table,
                next=next_)

        except Exception as e:
            logger.error(str(e))
            return DuckDBResponse(success=False, error=str(e))
