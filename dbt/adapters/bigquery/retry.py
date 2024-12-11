from typing import Callable, Optional

from google.api_core.future.polling import DEFAULT_POLLING
from google.api_core.retry import Retry
from google.cloud.bigquery.retry import DEFAULT_JOB_RETRY
from google.cloud.exceptions import BadRequest
from requests.exceptions import ConnectionError

from dbt.adapters.contracts.connection import Connection, ConnectionState
from dbt.adapters.events.logging import AdapterLogger
from dbt.adapters.exceptions.connection import FailedToConnectError

from dbt.adapters.bigquery.clients import create_bigquery_client
from dbt.adapters.bigquery.credentials import BigQueryCredentials


_logger = AdapterLogger("BigQuery")

_MINUTE = 60.0
_DAY = 24 * 60 * 60.0


class RetryFactory:

    def __init__(self, credentials: BigQueryCredentials) -> None:
        self._retries = credentials.job_retries or 0
        self._job_creation_timeout = credentials.job_creation_timeout_seconds
        self._job_execution_timeout = credentials.job_execution_timeout_seconds
        self._job_deadline = credentials.job_retry_deadline_seconds

    def create_job_creation_timeout(self) -> float:
        return self._job_creation_timeout or 1 * _MINUTE

    def create_job_execution_timeout(self, fallback: float = 1 * _DAY) -> float:
        return self._job_execution_timeout or fallback

    def create_job_execution_retry(self) -> Retry:
        return DEFAULT_JOB_RETRY.with_timeout(self.create_job_execution_timeout(5 * _MINUTE))

    def create_job_execution_polling(self, model_timeout: Optional[float] = None) -> Retry:
        return DEFAULT_POLLING.with_timeout(model_timeout or self.create_job_execution_timeout())

    def create_job_execution_retry_with_reopen(self, connection: Connection) -> Retry:
        """
        This strategy mimics what was accomplished with _retry_and_handle
        """

        retry = DEFAULT_JOB_RETRY.with_delay(maximum=3.0).with_predicate(
            _DeferredException(self._retries)
        )

        # there is no `with_on_error` method, but we want to retain the defaults on `DEFAULT_JOB_RETRY
        retry._on_error = _create_reopen_on_error(connection)

        # don't override the default deadline to None if the user did not provide one,
        # the process will never end
        if deadline := self._job_deadline:
            return retry.with_deadline(deadline)

        return retry


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

    def on_error(error: Exception) -> None:
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
    """
    Extend the default predicate `_job_should_retry` to include BadRequest

    Because `_job_should_retry` is private, take the predicate directly off of `DEFAULT_JOB_RETRY`.
    This is expected to be more stable.
    """

    # this is effectively an or, but it's more readable, especially if we add more in the future
    if isinstance(error, BadRequest):
        return True

    return DEFAULT_JOB_RETRY._predicate(error)
