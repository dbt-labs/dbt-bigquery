from typing import Callable, Optional

from google.api_core.exceptions import Forbidden
from google.api_core.future.polling import DEFAULT_POLLING
from google.api_core.retry import Retry
from google.cloud.bigquery.retry import DEFAULT_RETRY
from google.cloud.exceptions import BadGateway, BadRequest, ServerError
from requests.exceptions import ConnectionError

from dbt.adapters.contracts.connection import Connection, ConnectionState
from dbt.adapters.events.logging import AdapterLogger
from dbt.adapters.exceptions.connection import FailedToConnectError

from dbt.adapters.bigquery.clients import create_bigquery_client
from dbt.adapters.bigquery.credentials import BigQueryCredentials


_logger = AdapterLogger("BigQuery")


_SECOND = 1.0
_MINUTE = 60 * _SECOND
_HOUR = 60 * _MINUTE
_DAY = 24 * _HOUR
_DEFAULT_INITIAL_DELAY = _SECOND
_DEFAULT_MAXIMUM_DELAY = 3 * _SECOND
_DEFAULT_POLLING_MAXIMUM_DELAY = 10 * _SECOND


class RetryFactory:

    def __init__(self, credentials: BigQueryCredentials) -> None:
        self._retries = credentials.job_retries or 0
        self._job_creation_timeout = credentials.job_creation_timeout_seconds
        self._job_execution_timeout = credentials.job_execution_timeout_seconds
        self._job_deadline = credentials.job_retry_deadline_seconds

    def create_job_creation_timeout(self, fallback: float = _MINUTE) -> float:
        return (
            self._job_creation_timeout or fallback
        )  # keep _MINUTE here so it's not overridden by passing fallback=None

    def create_job_execution_timeout(self, fallback: float = _DAY) -> float:
        return (
            self._job_execution_timeout or fallback
        )  # keep _DAY here so it's not overridden by passing fallback=None

    def create_retry(self, fallback: Optional[float] = None) -> Retry:
        return DEFAULT_RETRY.with_timeout(self._job_execution_timeout or fallback or _DAY)

    def create_polling(self, model_timeout: Optional[float] = None) -> Retry:
        return DEFAULT_POLLING.with_timeout(model_timeout or self._job_execution_timeout or _DAY)

    def create_reopen_with_deadline(self, connection: Connection) -> Retry:
        """
        This strategy mimics what was accomplished with _retry_and_handle
        """
        return Retry(
            predicate=_DeferredException(self._retries),
            initial=_DEFAULT_INITIAL_DELAY,
            maximum=_DEFAULT_MAXIMUM_DELAY,
            deadline=self._job_deadline,
            on_error=_create_reopen_on_error(connection),
        )


class _DeferredException:
    """
    Count ALL errors, not just retryable errors, up to a threshold.
    Raise the next error, regardless of whether it is retryable.
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

        # if the error is retryable, and we haven't breached the threshold, log and continue
        if _is_retryable(error) and self._error_count <= self._retries:
            _logger.debug(
                f"Retry attempt {self._error_count} of {self._retries} after error: {repr(error)}"
            )
            return True

        # otherwise raise
        return False


def _create_reopen_on_error(connection: Connection) -> Callable[[Exception], None]:

    def on_error(error: Exception):
        if isinstance(error, (ConnectionResetError, ConnectionError)):
            _logger.warning("Reopening connection after {!r}".format(error))
            connection.handle.close()

            try:
                connection.handle = create_bigquery_client(connection.credentials)
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
    if isinstance(
        error, (BadGateway, BadRequest, ConnectionError, ConnectionResetError, ServerError)
    ):
        return True
    elif isinstance(error, Forbidden) and any(
        e["reason"] == "rateLimitExceeded" for e in error.errors
    ):
        return True
    return False
