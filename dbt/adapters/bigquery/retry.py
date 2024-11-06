from typing import Callable, Optional

from google.api_core import retry
from google.api_core.exceptions import Forbidden
from google.api_core.future.polling import POLLING_PREDICATE
from google.cloud.exceptions import BadGateway, BadRequest, ServerError
from requests.exceptions import ConnectionError

from dbt.adapters.contracts.connection import Connection, ConnectionState
from dbt.adapters.events.logging import AdapterLogger
from dbt.adapters.exceptions.connection import FailedToConnectError

from dbt.adapters.bigquery.credentials import BigQueryCredentials
from dbt.adapters.bigquery.clients import bigquery_client

_logger = AdapterLogger("BigQuery")


REOPENABLE_ERRORS = (
    ConnectionResetError,
    ConnectionError,
)


RETRYABLE_ERRORS = (
    ServerError,
    BadRequest,
    BadGateway,
    ConnectionResetError,
    ConnectionError,
)


class RetryFactory:

    _DEFAULT_INITIAL_DELAY = 1.0  # seconds
    _DEFAULT_MAXIMUM_DELAY = 3.0  # seconds

    def __init__(self, credentials: BigQueryCredentials) -> None:
        self._retries = credentials.job_retries or 0
        self.job_creation_timeout = credentials.job_creation_timeout_seconds
        self.job_execution_timeout = credentials.job_execution_timeout_seconds
        self.job_deadline = credentials.job_retry_deadline_seconds

    def deadline(self, connection: Connection) -> retry.Retry:
        """
        This strategy mimics what was accomplished with _retry_and_handle
        """
        return retry.Retry(
            predicate=self._buffered_predicate(),
            initial=self._DEFAULT_INITIAL_DELAY,
            maximum=self._DEFAULT_MAXIMUM_DELAY,
            timeout=self.job_deadline,
            on_error=_on_error(connection),
        )

    def job_execution(self, connection: Connection) -> retry.Retry:
        """
        This strategy mimics what was accomplished with _retry_and_handle
        """
        return retry.Retry(
            predicate=self._buffered_predicate(),
            initial=self._DEFAULT_INITIAL_DELAY,
            maximum=self._DEFAULT_MAXIMUM_DELAY,
            timeout=self.job_execution_timeout,
            on_error=_on_error(connection),
        )

    def job_execution_capped(self, connection: Connection) -> retry.Retry:
        """
        This strategy mimics what was accomplished with _retry_and_handle
        """
        return retry.Retry(
            predicate=self._buffered_predicate(),
            timeout=self.job_execution_timeout or 300,
            on_error=_on_error(connection),
        )

    def polling(self, timeout: Optional[float] = None) -> retry.Retry:
        """
        This strategy mimics what was accomplished with _retry_and_handle
        """
        return retry.Retry(
            predicate=POLLING_PREDICATE,
            minimum=1.0,
            maximum=10.0,
            timeout=timeout or self.job_execution_timeout or 60 * 60 * 24,
        )

    def _buffered_predicate(self) -> Callable[[Exception], bool]:
        class BufferedPredicate:
            """
            Count ALL errors, not just retryable errors, up to a threshold
            then raises the next error, regardless of whether it is retryable.

            Was previously called _ErrorCounter.
            """

            def __init__(self, retries: int) -> None:
                self._retries: int = retries
                self._error_count = 0

            def __call__(self, error: Exception) -> bool:
                # exit immediately if the user does not want retries
                if self._retries == 0:
                    return False

                # count all errors
                self._error_count += 1

                # if the error is retryable and we haven't breached the threshold, log and continue
                if _is_retryable(error) and self._error_count <= self._retries:
                    _logger.debug(
                        f"Retry attempt {self._error_count} of { self._retries} after error: {repr(error)}"
                    )
                    return True

                # otherwise raise
                return False

        return BufferedPredicate(self._retries)


def _on_error(connection: Connection) -> Callable[[Exception], None]:

    def on_error(error: Exception):
        if isinstance(error, REOPENABLE_ERRORS):
            _logger.warning("Reopening connection after {!r}".format(error))
            connection.handle.close()

            try:
                connection.handle = bigquery_client(connection.credentials)
                connection.state = ConnectionState.OPEN

            except Exception as e:
                _logger.debug(
                    f"""Got an error when attempting to create a bigquery " "client: '{e}'"""
                )
                connection.handle = None
                connection.state = ConnectionState.FAIL
                raise FailedToConnectError(str(e))

    return on_error


def _is_retryable(error: Exception) -> bool:
    """Return true for errors that are unlikely to occur again if retried."""
    if isinstance(error, RETRYABLE_ERRORS):
        return True
    elif isinstance(error, Forbidden) and any(
        e["reason"] == "rateLimitExceeded" for e in error.errors
    ):
        return True
    return False


class _BufferedPredicate:
    """Counts errors seen up to a threshold then raises the next error."""

    def __init__(self, retries: int) -> None:
        self._retries = retries
        self._error_count = 0

    def count_error(self, error):
        if self._retries == 0:
            return False  # Don't log
        self._error_count += 1
        if _is_retryable(error) and self._error_count <= self._retries:
            _logger.debug(
                "Retry attempt {} of {} after error: {}".format(
                    self._error_count, self._retries, repr(error)
                )
            )
            return True
        else:
            return False
