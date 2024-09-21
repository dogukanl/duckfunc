import abc
import json
import structlog
from typing import Self
from ..models import DuckDBResponse, DuckDBRequest, DuckDBQuery

logger = structlog.get_logger()


class Provider(abc.ABC):

    @abc.abstractmethod
    def perform_request(self: Self, request: DuckDBRequest,
                        *args, **kwargs) -> DuckDBResponse:
        pass

    def query(self: Self,
              sql: str,
              limit: int = None,
              offset: int = None,
              alias=None,
              params=None) -> DuckDBResponse:

        request = DuckDBRequest(
            context=None,
            query=DuckDBQuery(sql=sql, limit=limit, offset=offset, alias=alias, params=params))

        logger.debug(f"running: {request.query.sql}")

        response = self.perform_request(request)

        while n := response.next:
            logger.debug(
                "querying: "
                f"offset={request.query.offset}, "
                f"limit={request.query.limit}"
            )
            response += self.perform_request(n)

        return response
