import json
import werkzeug
import base64
import flask
import flask.typing
import functions_framework
import structlog
import traceback
from duckfunc import DuckDBRequest, DuckDBResponse, DuckDBQuery, handle

logger = structlog.get_logger()


@functions_framework.http
def quack(request: flask.Request) -> flask.typing.ResponseValue:

    response: str = None
    status_code: int = None

    try:
        req = DuckDBRequest(**request.json)
        res = handle(req)

        response = res.model_dump_json()
        status_code = 200 if res.success else 400

    except werkzeug.exceptions.BadRequest as e:
        res = DuckDBResponse(success=False, error="BadRequest")

        response = res.model_dump_json()
        status_code = 400

    except Exception as e:
        stacktrace = traceback.format_exc()
        logger.error(event=str(e), stacktrace=stacktrace, type=type(e))

        res = DuckDBResponse(success=False, error="InternalServerError")

        response = res.model_dump_json()
        status_code = 500

    finally:
        return response, status_code
