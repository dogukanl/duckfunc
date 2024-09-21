import os
import structlog
from typing import Self
from google.oauth2.service_account import IDTokenCredentials
from google.auth.credentials import Credentials
from google.auth.environment_vars import CREDENTIALS
from google.auth.transport.requests import AuthorizedSession, Request
from duckfunc.models import DuckDBRequest, DuckDBResponse
from .provider import Provider

logger = structlog.get_logger()


class CloudFunctionsProvider(Provider):
    def __init__(
            self,
            url: str,
            credentials: Credentials = None,
            timeout: int = 60):

        self._url = url
        self._timeout = timeout

        if credentials is None:

            logger.info(
                "credentials were not supplied,"
                f"looking for `{CREDENTIALS}` environment variable"
            )

            credentials_file = os.environ.get(CREDENTIALS)

            if credentials_file is None:
                raise RuntimeError("cannot set credentials")

            credentials = IDTokenCredentials.from_service_account_file(
                credentials_file, target_audience=self._url)

            credentials.refresh(Request())

            logger.info(f"credentials loaded from {credentials_file}")

        self._session = AuthorizedSession(credentials=credentials)

    def perform_request(
            self: Self,
            request: DuckDBRequest,
            *args,
            **kwargs) -> DuckDBResponse:

        response = self._session.post(
            url=self._url,
            json=request.model_dump(),
            timeout=self._timeout,
        )
        try:
            return DuckDBResponse(**response.json())
        except Exception as e:
            logger.info(response.content)
            raise e
