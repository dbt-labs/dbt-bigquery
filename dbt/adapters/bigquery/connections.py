from collections import defaultdict
from concurrent.futures import TimeoutError
from contextlib import contextmanager
from dataclasses import dataclass
import json
from multiprocessing.context import SpawnContext
import re
from typing import Any, Dict, Generator, Hashable, List, Optional, Tuple, TYPE_CHECKING
import uuid

from google.auth.exceptions import RefreshError
from google.cloud.bigquery import Client, QueryJob, QueryJobConfig, QueryPriority
from google.cloud.exceptions import BadRequest, Forbidden, NotFound

from dbt_common.events.contextvars import get_node_info
from dbt_common.events.functions import fire_event
from dbt_common.exceptions import DbtDatabaseError, DbtRuntimeError
from dbt_common.invocation import get_invocation_id
from dbt.adapters.base import BaseConnectionManager
from dbt.adapters.contracts.connection import (
    AdapterRequiredConfig,
    AdapterResponse,
    Connection,
    ConnectionState,
)
from dbt.adapters.events.logging import AdapterLogger
from dbt.adapters.events.types import SQLQuery
from dbt.adapters.exceptions.connection import FailedToConnectError

from dbt.adapters.bigquery.clients import bigquery_client
from dbt.adapters.bigquery.credentials import Priority, BigQueryCredentials
from dbt.adapters.bigquery.services import RetryService

if TYPE_CHECKING:
    # Indirectly imported via agate_helper, which is lazy loaded further downfile.
    # Used by mypy for earlier type hints.
    import agate


logger = AdapterLogger("BigQuery")


BQ_QUERY_JOB_SPLIT = "-----Query Job SQL Follows-----"


@dataclass
class BigQueryAdapterResponse(AdapterResponse):
    bytes_processed: Optional[int] = None
    bytes_billed: Optional[int] = None
    location: Optional[str] = None
    project_id: Optional[str] = None
    job_id: Optional[str] = None
    slot_ms: Optional[int] = None


class BigQueryConnectionManager(BaseConnectionManager):
    TYPE = "bigquery"

    def __init__(self, profile: AdapterRequiredConfig, mp_context: SpawnContext) -> None:
        super().__init__(profile, mp_context)
        credentials: BigQueryCredentials = profile.credentials
        self.jobs_by_thread: Dict[Hashable, List[str]] = defaultdict(list)
        self._retry = RetryService(
            credentials.job_creation_timeout_seconds,
            credentials.job_execution_timeout_seconds,
            credentials.job_retry_deadline_seconds,
            credentials.job_retries,
        )

    def clear_transaction(self) -> None:
        """BigQuery does not support transactions"""
        pass

    @contextmanager
    def exception_handler(self, sql: str) -> Generator:
        try:
            yield
        except (BadRequest, Forbidden, NotFound) as e:
            if hasattr(e, "query_job"):
                logger.error(_query_job_url(e.query_job))
            raise DbtDatabaseError("\n".join([item["message"] for item in e.errors]))
        except RefreshError as e:
            message = (
                "Unable to generate access token, if you're using "
                "impersonate_service_account, make sure your "
                'initial account has the "roles/'
                'iam.serviceAccountTokenCreator" role on the '
                "account you are trying to impersonate.\n\n"
                f"{str(e)}"
            )
            raise DbtRuntimeError(message)
        except Exception as e:
            logger.debug("Unhandled error while running:\n{}".format(sql))
            logger.debug(e)
            if isinstance(e, DbtRuntimeError):
                # during a sql query, an internal to dbt exception was raised.
                # this sounds a lot like a signal handler and probably has
                # useful information, so raise it without modification.
                raise
            exc_message = str(e)
            # the google bigquery library likes to add the query log, which we
            # don't want to log. Hopefully they never change this!
            if BQ_QUERY_JOB_SPLIT in exc_message:
                exc_message = exc_message.split(BQ_QUERY_JOB_SPLIT)[0].strip()
            raise DbtRuntimeError(exc_message)

    def cancel_open(self) -> Optional[List[str]]:
        names = []
        this_connection = self.get_if_exists()
        with self.lock:
            for thread_id, connection in self.thread_connections.items():
                if connection is this_connection:
                    continue

                if connection.handle is not None and connection.state == ConnectionState.OPEN:
                    client: Client = connection.handle
                    for job_id in self.jobs_by_thread.get(thread_id, []):
                        with self.exception_handler(f"Cancel job: {job_id}"):
                            client.cancel_job(
                                job_id,
                                retry=self._retry.reopen_with_deadline(connection),
                            )
                    self.close(connection)

                if connection.name is not None:
                    names.append(connection.name)
        return names

    @classmethod
    def open(cls, connection: Connection) -> Connection:
        if connection.state == ConnectionState.OPEN:
            logger.debug("Connection is already open, skipping open.")
            return connection

        try:
            connection.handle = bigquery_client(connection.credentials)
            connection.state = ConnectionState.OPEN
            return connection

        except Exception as e:
            logger.debug(f"""Got an error when attempting to create a bigquery " "client: '{e}'""")
            connection.handle = None
            connection.state = ConnectionState.FAIL
            raise FailedToConnectError(str(e))

    def begin(self) -> None:
        """BigQuery does not support transactions"""
        pass

    def commit(self) -> None:
        """BigQuery does not support transactions"""
        pass

    @classmethod
    def close(cls, connection: Connection) -> Connection:
        connection.handle.close()
        connection.state = ConnectionState.CLOSED
        return connection

    def execute(
        self,
        sql: str,
        auto_begin: Optional[bool] = False,
        fetch: Optional[bool] = False,
        limit: Optional[int] = None,
    ) -> Tuple[BigQueryAdapterResponse, "agate.Table"]:
        sql = self._add_query_comment(sql)
        query_job, iterator = self.raw_execute(sql, limit=limit)

        response = _query_job_url(query_job)

        from dbt_common.clients import agate_helper

        if fetch:
            column_names = [field.name for field in iterator.schema]
            table = agate_helper.table_from_data_flat(iterator, column_names)
        else:
            table = agate_helper.empty_table()

        return response, table

    # ==============================
    # dbt-bigquery specific methods
    # ==============================

    def bigquery_client(self) -> Client:
        conn = self.get_thread_connection()
        return conn.handle

    def generate_job_id(self) -> str:
        # Generating a fresh job_id for every _query_and_results call to avoid job_id reuse.
        # Generating a job id instead of persisting a BigQuery-generated one after client.query is called.
        # Using BigQuery's job_id can lead to a race condition if a job has been started and a termination
        # is sent before the job_id was stored, leading to a failure to cancel the job.
        # By predetermining job_ids (uuid4), we can persist the job_id before the job has been kicked off.
        # Doing this, the race condition only leads to attempting to cancel a job that doesn't exist.
        job_id = str(uuid.uuid4())
        thread_id = self.get_thread_identifier()
        self.jobs_by_thread[thread_id].append(job_id)
        return job_id

    def raw_execute(
        self,
        sql,
        use_legacy_sql=False,
        limit: Optional[int] = None,
        dry_run: bool = False,
    ):
        conn = self.get_thread_connection()
        client: Client = conn.handle

        fire_event(SQLQuery(conn_name=conn.name, sql=sql, node_info=get_node_info()))

        labels = _get_labels_from_query_comment(self.profile, self.query_header)
        labels["dbt_invocation_id"] = get_invocation_id()

        job_params = {
            "use_legacy_sql": use_legacy_sql,
            "labels": labels,
            "dry_run": dry_run,
            "priority": (
                QueryPriority.BATCH
                if self.profile.credentials.priority == Priority.Batch
                else QueryPriority.INTERACTIVE
            ),
        }

        if maximum_bytes_billed := conn.credentials.maximum_bytes_billed:
            job_params["maximum_bytes_billed"] = maximum_bytes_billed
        job_config = QueryJobConfig(**job_params)

        with self.exception_handler(sql):

            query_job = client.query(
                query=sql,
                job_config=job_config,
                job_id=self.generate_job_id(),  # note, this disables retry since the job_id will have been used
                timeout=self._retry.job_creation_timeout(),
            )
            if all((query_job.location, query_job.job_id, query_job.project)):
                logger.debug(_query_job_url(query_job))
            try:
                iterator = query_job.result(
                    max_results=limit, timeout=self._retry.job_execution_timeout()
                )
            except TimeoutError:
                exc = f"Operation did not complete within the designated timeout of {self._retry.job_execution_timeout()} seconds."
                raise TimeoutError(exc)
            return query_job, iterator

    def query_job_defaults(self) -> Dict[str, Any]:
        labels = _get_labels_from_query_comment(self.profile, self.query_header)
        labels["dbt_invocation_id"] = get_invocation_id()

        config = {
            "labels": labels,
            "use_legacy_sql": False,
            "dry_run": False,
        }

        if self.profile.credentials.priority == Priority.Batch:
            config["priority"] = QueryPriority.BATCH
        else:
            config["priority"] = QueryPriority.INTERACTIVE

        if maximum_bytes_billed := self.profile.credentials.maximum_bytes_billed:
            config["maximum_bytes_billed"] = maximum_bytes_billed

        return config


def _get_labels_from_query_comment(profile, query_header):
    if (
        hasattr(profile, "query_comment")
        and profile.query_comment
        and profile.query_comment.job_label
        and query_header
    ):
        query_comment = query_header.comment.query_comment
        return _labels_from_query_comment(query_comment)

    return {}


def _labels_from_query_comment(comment: str) -> Dict:
    try:
        comment_labels = json.loads(comment)
    except (TypeError, ValueError):
        return {"query_comment": _sanitize_label(comment)}
    return {
        _sanitize_label(key): _sanitize_label(str(value)) for key, value in comment_labels.items()
    }


_SANITIZE_LABEL_PATTERN = re.compile(r"[^a-z0-9_-]")

_VALIDATE_LABEL_LENGTH_LIMIT = 63


def _sanitize_label(value: str) -> str:
    """Return a legal value for a BigQuery label."""
    value = value.strip().lower()
    value = _SANITIZE_LABEL_PATTERN.sub("_", value)
    return value[:_VALIDATE_LABEL_LENGTH_LIMIT]


def _query_job_url(query_job: QueryJob) -> str:
    return f"https://console.cloud.google.com/bigquery?project={query_job.project}&j=bq:{query_job.location}:{query_job.job_id}&page=queryresults"
