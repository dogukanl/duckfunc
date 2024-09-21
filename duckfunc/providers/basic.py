import requests
from typing import Self
from duckfunc.models import DuckDBQuery, DuckDBResponse, DuckDBRequest
from .provider import Provider


class BasicProvider(Provider):
    def __init__(self, endpoint: str, timeout: int = 60):
        self._endpoint = endpoint

        self._session = requests.Session()
        self._session.timeout = timeout

    def perform_request(self: Self, request: DuckDBRequest) -> DuckDBResponse:
        res = self._session.post(
            self._endpoint,
            json=request.model_dump())

        return DuckDBResponse(**res.json())
