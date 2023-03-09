import json
import re
from contextlib import contextmanager
from dataclasses import dataclass, field
from mashumaro.helper import pass_through

from functools import lru_cache
import agate
from requests.exceptions import ConnectionError
from typing import Optional, Any, Dict, Tuple

import google.auth
import google.auth.exceptions
import google.cloud.bigquery
import google.cloud.exceptions
from google.api_core import retry, client_info
from google.auth import impersonated_credentials
from google.oauth2 import (
    credentials as GoogleCredentials,
    service_account as GoogleServiceAccountCredentials,
)

from dbt.adapters.bigquery import gcloud
from dbt.clients import agate_helper
from dbt.config.profile import INVALID_PROFILE_MESSAGE
from dbt.tracking import active_user
from dbt.contracts.connection import ConnectionState, AdapterResponse
from dbt.exceptions import (
    FailedToConnectError,
    DbtRuntimeError,
    DbtDatabaseError,
    DbtProfileError,
)
from dbt.adapters.base import BaseConnectionManager, Credentials
from dbt.events import AdapterLogger
from dbt.events.functions import fire_event
from dbt.events.types import SQLQuery
from dbt.version import __version__ as dbt_version

from dbt.dataclass_schema import ExtensibleDbtClassMixin, StrEnum

logger = AdapterLogger("BigQuery")

BQ_QUERY_JOB_SPLIT = "-----Query Job SQL Follows-----"

WRITE_TRUNCATE = google.cloud.bigquery.job.WriteDisposition.WRITE_TRUNCATE

REOPENABLE_ERRORS = (
    ConnectionResetError,
    ConnectionError,
)

RETRYABLE_ERRORS = (
    google.cloud.exceptions.ServerError,
    google.cloud.exceptions.BadRequest,
    ConnectionResetError,
    ConnectionError,
)


@lru_cache()
def get_bigquery_defaults(scopes=None) -> Tuple[Any, Optional[str]]:
    """
    Returns (credentials, project_id)

    project_id is returned available from the environment; otherwise None
    """
    # Cached, because the underlying implementation shells out, taking ~1s
    try:
        credentials, _ = google.auth.default(scopes=scopes)
        return credentials, _
    except google.auth.exceptions.DefaultCredentialsError as e:
        raise DbtProfileError(INVALID_PROFILE_MESSAGE.format(error_string=e))


class Priority(StrEnum):
    Interactive = "interactive"
    Batch = "batch"


class BigQueryConnectionMethod(StrEnum):
    OAUTH = "oauth"
    SERVICE_ACCOUNT = "service-account"
    SERVICE_ACCOUNT_JSON = "service-account-json"
    OAUTH_SECRETS = "oauth-secrets"


@dataclass
class BigQueryAdapterResponse(AdapterResponse):
    bytes_processed: Optional[int] = None
    location: Optional[str] = None
    project_id: Optional[str] = None
    job_id: Optional[str] = None
    slot_ms: Optional[int] = None


@dataclass
class DataprocBatchConfig(ExtensibleDbtClassMixin):
    def __init__(self, batch_config):
        self.batch_config = batch_config


@dataclass
class BigQueryCredentials(Credentials):
    method: BigQueryConnectionMethod
    # BigQuery allows an empty database / project, where it defers to the
    # environment for the project
    database: Optional[str]  # type: ignore
    execution_project: Optional[str] = None
    location: Optional[str] = None
    priority: Optional[Priority] = None
    maximum_bytes_billed: Optional[int] = None
    impersonate_service_account: Optional[str] = None

    job_retry_deadline_seconds: Optional[int] = None
    job_retries: Optional[int] = 1
    job_creation_timeout_seconds: Optional[int] = None
    job_execution_timeout_seconds: Optional[int] = None

    # Keyfile json creds
    keyfile: Optional[str] = None
    keyfile_json: Optional[Dict[str, Any]] = None

    # oauth-secrets
    token: Optional[str] = None
    refresh_token: Optional[str] = None
    client_id: Optional[str] = None
    client_secret: Optional[str] = None
    token_uri: Optional[str] = None

    dataproc_region: Optional[str] = None
    dataproc_cluster_name: Optional[str] = None
    gcs_bucket: Optional[str] = None

    dataproc_batch: Optional[DataprocBatchConfig] = field(
        metadata={
            "serialization_strategy": pass_through,
        },
        default=None,
    )

    scopes: Optional[Tuple[str, ...]] = (
        "https://www.googleapis.com/auth/bigquery",
        "https://www.googleapis.com/auth/cloud-platform",
        "https://www.googleapis.com/auth/drive",
    )

    _ALIASES = {
        # 'legacy_name': 'current_name'
        "project": "database",
        "dataset": "schema",
        "target_project": "target_database",
        "target_dataset": "target_schema",
        "retries": "job_retries",
        "timeout_seconds": "job_execution_timeout_seconds",
    }

    @property
    def type(self):
        return "bigquery"

    @property
    def unique_field(self):
        return self.database

    def _connection_keys(self):
        return (
            "method",
            "database",
            "schema",
            "location",
            "priority",
            "timeout_seconds",
            "maximum_bytes_billed",
            "execution_project",
            "job_retry_deadline_seconds",
            "job_retries",
            "job_creation_timeout_seconds",
            "job_execution_timeout_seconds",
            "gcs_bucket",
        )

    @classmethod
    def __pre_deserialize__(cls, d: Dict[Any, Any]) -> Dict[Any, Any]:
        # We need to inject the correct value of the database (aka project) at
        # this stage, ref
        # https://github.com/dbt-labs/dbt/pull/2908#discussion_r532927436.

        # `database` is an alias of `project` in BigQuery
        if "database" not in d:
            _, database = get_bigquery_defaults()
            d["database"] = database
        # `execution_project` default to dataset/project
        if "execution_project" not in d:
            d["execution_project"] = d["database"]
        return d


class BigQueryConnectionManager(BaseConnectionManager):
    TYPE = "bigquery"

    DEFAULT_INITIAL_DELAY = 1.0  # Seconds
    DEFAULT_MAXIMUM_DELAY = 3.0  # Seconds

    @classmethod
    def handle_error(cls, error, message):
        error_msg = "\n".join([item["message"] for item in error.errors])
        if hasattr(error, "query_job"):
            logger.error(
                cls._bq_job_link(
                    error.query_job.location, error.query_job.project, error.query_job.job_id
                )
            )
        raise DbtDatabaseError(error_msg)

    def clear_transaction(self):
        pass

    @contextmanager
    def exception_handler(self, sql):
        try:
            yield

        except google.cloud.exceptions.BadRequest as e:
            message = "Bad request while running query"
            self.handle_error(e, message)

        except google.cloud.exceptions.Forbidden as e:
            message = "Access denied while running query"
            self.handle_error(e, message)

        except google.auth.exceptions.RefreshError as e:
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

    def cancel_open(self) -> None:
        pass

    @classmethod
    def close(cls, connection):
        connection.state = ConnectionState.CLOSED

        return connection

    def begin(self):
        pass

    def commit(self):
        pass

    def format_bytes(self, num_bytes):
        if num_bytes:
            for unit in ["Bytes", "KB", "MB", "GB", "TB", "PB"]:
                if abs(num_bytes) < 1024.0:
                    return f"{num_bytes:3.1f} {unit}"
                num_bytes /= 1024.0

            num_bytes *= 1024.0
            return f"{num_bytes:3.1f} {unit}"

        else:
            return num_bytes

    def format_rows_number(self, rows_number):
        for unit in ["", "k", "m", "b", "t"]:
            if abs(rows_number) < 1000.0:
                return f"{rows_number:3.1f}{unit}".strip()
            rows_number /= 1000.0

        rows_number *= 1000.0
        return f"{rows_number:3.1f}{unit}".strip()

    @classmethod
    def get_google_credentials(cls, profile_credentials) -> GoogleCredentials:
        method = profile_credentials.method
        creds = GoogleServiceAccountCredentials.Credentials

        if method == BigQueryConnectionMethod.OAUTH:
            credentials, _ = get_bigquery_defaults(scopes=profile_credentials.scopes)
            return credentials

        elif method == BigQueryConnectionMethod.SERVICE_ACCOUNT:
            keyfile = profile_credentials.keyfile
            return creds.from_service_account_file(keyfile, scopes=profile_credentials.scopes)

        elif method == BigQueryConnectionMethod.SERVICE_ACCOUNT_JSON:
            details = profile_credentials.keyfile_json
            return creds.from_service_account_info(details, scopes=profile_credentials.scopes)

        elif method == BigQueryConnectionMethod.OAUTH_SECRETS:
            return GoogleCredentials.Credentials(
                token=profile_credentials.token,
                refresh_token=profile_credentials.refresh_token,
                client_id=profile_credentials.client_id,
                client_secret=profile_credentials.client_secret,
                token_uri=profile_credentials.token_uri,
                scopes=profile_credentials.scopes,
            )

        error = 'Invalid `method` in profile: "{}"'.format(method)
        raise FailedToConnectError(error)

    @classmethod
    def get_impersonated_credentials(cls, profile_credentials):
        source_credentials = cls.get_google_credentials(profile_credentials)
        return impersonated_credentials.Credentials(
            source_credentials=source_credentials,
            target_principal=profile_credentials.impersonate_service_account,
            target_scopes=list(profile_credentials.scopes),
            lifetime=(profile_credentials.job_execution_timeout_seconds or 300),
        )

    @classmethod
    def get_credentials(cls, profile_credentials):
        if profile_credentials.impersonate_service_account:
            return cls.get_impersonated_credentials(profile_credentials)
        else:
            return cls.get_google_credentials(profile_credentials)

    @classmethod
    @retry.Retry()  # google decorator. retries on transient errors with exponential backoff
    def get_bigquery_client(cls, profile_credentials):
        creds = cls.get_credentials(profile_credentials)
        execution_project = profile_credentials.execution_project
        location = getattr(profile_credentials, "location", None)

        info = client_info.ClientInfo(user_agent=f"dbt-{dbt_version}")
        return google.cloud.bigquery.Client(
            execution_project,
            creds,
            location=location,
            client_info=info,
        )

    @classmethod
    def open(cls, connection):
        if connection.state == "open":
            logger.debug("Connection is already open, skipping open.")
            return connection

        try:
            handle = cls.get_bigquery_client(connection.credentials)

        except google.auth.exceptions.DefaultCredentialsError:
            logger.info("Please log into GCP to continue")
            gcloud.setup_default_credentials()

            handle = cls.get_bigquery_client(connection.credentials)

        except Exception as e:
            logger.debug(
                "Got an error when attempting to create a bigquery " "client: '{}'".format(e)
            )

            connection.handle = None
            connection.state = "fail"

            raise FailedToConnectError(str(e))

        connection.handle = handle
        connection.state = "open"
        return connection

    @classmethod
    def get_job_execution_timeout_seconds(cls, conn):
        credentials = conn.credentials
        return credentials.job_execution_timeout_seconds

    @classmethod
    def get_job_retries(cls, conn) -> int:
        credentials = conn.credentials
        return credentials.job_retries

    @classmethod
    def get_job_creation_timeout_seconds(cls, conn):
        credentials = conn.credentials
        return credentials.job_creation_timeout_seconds

    @classmethod
    def get_job_retry_deadline_seconds(cls, conn):
        credentials = conn.credentials
        return credentials.job_retry_deadline_seconds

    @classmethod
    def get_table_from_response(cls, resp):
        column_names = [field.name for field in resp.schema]
        return agate_helper.table_from_data_flat(resp, column_names)

    def raw_execute(self, sql, fetch=False, *, use_legacy_sql=False):
        conn = self.get_thread_connection()
        client = conn.handle

        fire_event(SQLQuery(conn_name=conn.name, sql=sql))
        if (
            hasattr(self.profile, "query_comment")
            and self.profile.query_comment
            and self.profile.query_comment.job_label
        ):
            query_comment = self.query_header.comment.query_comment
            labels = self._labels_from_query_comment(query_comment)
        else:
            labels = {}

        if active_user:
            labels["dbt_invocation_id"] = active_user.invocation_id

        job_params = {"use_legacy_sql": use_legacy_sql, "labels": labels}

        priority = conn.credentials.priority
        if priority == Priority.Batch:
            job_params["priority"] = google.cloud.bigquery.QueryPriority.BATCH
        else:
            job_params["priority"] = google.cloud.bigquery.QueryPriority.INTERACTIVE

        maximum_bytes_billed = conn.credentials.maximum_bytes_billed
        if maximum_bytes_billed is not None and maximum_bytes_billed != 0:
            job_params["maximum_bytes_billed"] = maximum_bytes_billed

        job_creation_timeout = self.get_job_creation_timeout_seconds(conn)
        job_execution_timeout = self.get_job_execution_timeout_seconds(conn)

        def fn():
            return self._query_and_results(
                client,
                sql,
                job_params,
                job_creation_timeout=job_creation_timeout,
                job_execution_timeout=job_execution_timeout,
            )

        query_job, iterator = self._retry_and_handle(msg=sql, conn=conn, fn=fn)

        return query_job, iterator

    def execute(
        self, sql, auto_begin=False, fetch=None
    ) -> Tuple[BigQueryAdapterResponse, agate.Table]:
        sql = self._add_query_comment(sql)
        # auto_begin is ignored on bigquery, and only included for consistency
        query_job, iterator = self.raw_execute(sql, fetch=fetch)

        if fetch:
            table = self.get_table_from_response(iterator)
        else:
            table = agate_helper.empty_table()

        message = "OK"
        code = None
        num_rows = None
        bytes_processed = None
        location = None
        job_id = None
        project_id = None
        num_rows_formatted = None
        processed_bytes = None
        slot_ms = None

        if query_job.statement_type == "CREATE_VIEW":
            code = "CREATE VIEW"

        elif query_job.statement_type == "CREATE_TABLE_AS_SELECT":
            code = "CREATE TABLE"
            conn = self.get_thread_connection()
            client = conn.handle
            query_table = client.get_table(query_job.destination)
            num_rows = query_table.num_rows

        elif query_job.statement_type == "SCRIPT":
            code = "SCRIPT"

        elif query_job.statement_type in ["INSERT", "DELETE", "MERGE", "UPDATE"]:
            code = query_job.statement_type
            num_rows = query_job.num_dml_affected_rows

        elif query_job.statement_type == "SELECT":
            code = "SELECT"
            conn = self.get_thread_connection()
            client = conn.handle
            # use anonymous table for num_rows
            query_table = client.get_table(query_job.destination)
            num_rows = query_table.num_rows

        # set common attributes
        bytes_processed = query_job.total_bytes_processed
        slot_ms = query_job.slot_millis
        processed_bytes = self.format_bytes(bytes_processed)
        location = query_job.location
        job_id = query_job.job_id
        project_id = query_job.project
        if num_rows is not None:
            num_rows_formatted = self.format_rows_number(num_rows)
            message = f"{code} ({num_rows_formatted} rows, {processed_bytes} processed)"
        elif bytes_processed is not None:
            message = f"{code} ({processed_bytes} processed)"
        else:
            message = f"{code}"

        if location is not None and job_id is not None and project_id is not None:
            logger.debug(self._bq_job_link(location, project_id, job_id))

        response = BigQueryAdapterResponse(  # type: ignore[call-arg]
            _message=message,
            rows_affected=num_rows,
            code=code,
            bytes_processed=bytes_processed,
            location=location,
            project_id=project_id,
            job_id=job_id,
            slot_ms=slot_ms,
        )

        return response, table

    @staticmethod
    def _bq_job_link(location, project_id, job_id) -> str:
        return f"https://console.cloud.google.com/bigquery?project={project_id}&j=bq:{location}:{job_id}&page=queryresults"

    def get_partitions_metadata(self, table):
        def standard_to_legacy(table):
            return table.project + ":" + table.dataset + "." + table.identifier

        legacy_sql = "SELECT * FROM [" + standard_to_legacy(table) + "$__PARTITIONS_SUMMARY__]"

        sql = self._add_query_comment(legacy_sql)
        # auto_begin is ignored on bigquery, and only included for consistency
        _, iterator = self.raw_execute(sql, fetch="fetch_result", use_legacy_sql=True)
        return self.get_table_from_response(iterator)

    def copy_bq_table(self, source, destination, write_disposition):
        conn = self.get_thread_connection()
        client = conn.handle

        # -------------------------------------------------------------------------------
        #  BigQuery allows to use copy API using two different formats:
        #  1. client.copy_table(source_table_id, destination_table_id)
        #     where source_table_id = "your-project.source_dataset.source_table"
        #  2. client.copy_table(source_table_ids, destination_table_id)
        #     where source_table_ids = ["your-project.your_dataset.your_table_name", ...]
        #  Let's use uniform function call and always pass list there
        # -------------------------------------------------------------------------------
        if type(source) is not list:
            source = [source]

        source_ref_array = [
            self.table_ref(src_table.database, src_table.schema, src_table.table)
            for src_table in source
        ]
        destination_ref = self.table_ref(
            destination.database, destination.schema, destination.table
        )

        logger.debug(
            'Copying table(s) "{}" to "{}" with disposition: "{}"',
            ", ".join(source_ref.path for source_ref in source_ref_array),
            destination_ref.path,
            write_disposition,
        )

        def copy_and_results():
            job_config = google.cloud.bigquery.CopyJobConfig(write_disposition=write_disposition)
            copy_job = client.copy_table(source_ref_array, destination_ref, job_config=job_config)
            timeout = self.get_job_execution_timeout_seconds(conn) or 300
            iterator = copy_job.result(timeout=timeout)
            return copy_job, iterator

        self._retry_and_handle(
            msg='copy table "{}" to "{}"'.format(
                ", ".join(source_ref.path for source_ref in source_ref_array),
                destination_ref.path,
            ),
            conn=conn,
            fn=copy_and_results,
        )

    @staticmethod
    def dataset_ref(database, schema):
        return google.cloud.bigquery.DatasetReference(project=database, dataset_id=schema)

    @staticmethod
    def table_ref(database, schema, table_name):
        dataset_ref = google.cloud.bigquery.DatasetReference(database, schema)
        return google.cloud.bigquery.TableReference(dataset_ref, table_name)

    def get_bq_table(self, database, schema, identifier):
        """Get a bigquery table for a schema/model."""
        conn = self.get_thread_connection()
        # backwards compatibility: fill in with defaults if not specified
        database = database or conn.credentials.database
        schema = schema or conn.credentials.schema
        table_ref = self.table_ref(database, schema, identifier)
        return conn.handle.get_table(table_ref)

    def drop_dataset(self, database, schema):
        conn = self.get_thread_connection()
        dataset_ref = self.dataset_ref(database, schema)
        client = conn.handle

        def fn():
            return client.delete_dataset(dataset_ref, delete_contents=True, not_found_ok=True)

        self._retry_and_handle(msg="drop dataset", conn=conn, fn=fn)

    def create_dataset(self, database, schema):
        conn = self.get_thread_connection()
        client = conn.handle
        dataset_ref = self.dataset_ref(database, schema)

        def fn():
            return client.create_dataset(dataset_ref, exists_ok=True)

        self._retry_and_handle(msg="create dataset", conn=conn, fn=fn)

    def _query_and_results(
        self,
        client,
        sql,
        job_params,
        job_creation_timeout=None,
        job_execution_timeout=None,
    ):
        """Query the client and wait for results."""
        # Cannot reuse job_config if destination is set and ddl is used
        job_config = google.cloud.bigquery.QueryJobConfig(**job_params)
        query_job = client.query(query=sql, job_config=job_config, timeout=job_creation_timeout)
        iterator = query_job.result(timeout=job_execution_timeout)

        return query_job, iterator

    def _retry_and_handle(self, msg, conn, fn):
        """retry a function call within the context of exception_handler."""

        def reopen_conn_on_error(error):
            if isinstance(error, REOPENABLE_ERRORS):
                logger.warning("Reopening connection after {!r}".format(error))
                self.close(conn)
                self.open(conn)
                return

        with self.exception_handler(msg):
            return retry.retry_target(
                target=fn,
                predicate=_ErrorCounter(self.get_job_retries(conn)).count_error,
                sleep_generator=self._retry_generator(),
                deadline=self.get_job_retry_deadline_seconds(conn),
                on_error=reopen_conn_on_error,
            )

    def _retry_generator(self):
        """Generates retry intervals that exponentially back off."""
        return retry.exponential_sleep_generator(
            initial=self.DEFAULT_INITIAL_DELAY, maximum=self.DEFAULT_MAXIMUM_DELAY
        )

    def _labels_from_query_comment(self, comment: str) -> Dict:
        try:
            comment_labels = json.loads(comment)
        except (TypeError, ValueError):
            return {"query_comment": _sanitize_label(comment)}
        return {
            _sanitize_label(key): _sanitize_label(str(value))
            for key, value in comment_labels.items()
        }


class _ErrorCounter(object):
    """Counts errors seen up to a threshold then raises the next error."""

    def __init__(self, retries):
        self.retries = retries
        self.error_count = 0

    def count_error(self, error):
        if self.retries == 0:
            return False  # Don't log
        self.error_count += 1
        if _is_retryable(error) and self.error_count <= self.retries:
            logger.debug(
                "Retry attempt {} of {} after error: {}".format(
                    self.error_count, self.retries, repr(error)
                )
            )
            return True
        else:
            return False


def _is_retryable(error):
    """Return true for errors that are unlikely to occur again if retried."""
    if isinstance(error, RETRYABLE_ERRORS):
        return True
    elif isinstance(error, google.api_core.exceptions.Forbidden) and any(
        e["reason"] == "rateLimitExceeded" for e in error.errors
    ):
        return True
    return False


_SANITIZE_LABEL_PATTERN = re.compile(r"[^a-z0-9_-]")

_VALIDATE_LABEL_LENGTH_LIMIT = 63


def _sanitize_label(value: str) -> str:
    """Return a legal value for a BigQuery label."""
    value = value.strip().lower()
    value = _SANITIZE_LABEL_PATTERN.sub("_", value)
    return value[:_VALIDATE_LABEL_LENGTH_LIMIT]
