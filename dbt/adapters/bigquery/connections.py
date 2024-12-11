from collections import defaultdict
from concurrent.futures import TimeoutError
from contextlib import contextmanager
from dataclasses import dataclass
import json
from multiprocessing.context import SpawnContext
import re
from typing import Dict, Hashable, List, Optional, Tuple, TYPE_CHECKING
import uuid

from google.auth.exceptions import RefreshError
from google.cloud.bigquery import (
    Client,
    CopyJobConfig,
    Dataset,
    DatasetReference,
    LoadJobConfig,
    QueryJobConfig,
    QueryPriority,
    SchemaField,
    Table,
    TableReference,
)
from google.cloud.exceptions import BadRequest, Forbidden, NotFound

from dbt_common.events.contextvars import get_node_info
from dbt_common.events.functions import fire_event
from dbt_common.exceptions import DbtDatabaseError, DbtRuntimeError
from dbt_common.invocation import get_invocation_id
from dbt.adapters.base import BaseConnectionManager
from dbt.adapters.contracts.connection import (
    AdapterRequiredConfig,
    AdapterResponse,
    ConnectionState,
)
from dbt.adapters.events.logging import AdapterLogger
from dbt.adapters.events.types import SQLQuery
from dbt.adapters.exceptions.connection import FailedToConnectError

from dbt.adapters.bigquery.clients import create_bigquery_client
from dbt.adapters.bigquery.credentials import Priority
from dbt.adapters.bigquery.retry import RetryFactory

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

    def __init__(self, profile: AdapterRequiredConfig, mp_context: SpawnContext):
        super().__init__(profile, mp_context)
        self.jobs_by_thread: Dict[Hashable, List[str]] = defaultdict(list)
        self._retry = RetryFactory(profile.credentials)

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

        except BadRequest as e:
            message = "Bad request while running query"
            self.handle_error(e, message)

        except Forbidden as e:
            message = "Access denied while running query"
            self.handle_error(e, message)

        except NotFound as e:
            message = "Not found while running query"
            self.handle_error(e, message)

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

    def cancel_open(self):
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
                                retry=self._retry.create_reopen_with_deadline(connection),
                            )
                    self.close(connection)

                if connection.name is not None:
                    names.append(connection.name)
        return names

    @classmethod
    def close(cls, connection):
        connection.handle.close()
        connection.state = ConnectionState.CLOSED

        return connection

    def begin(self):
        pass

    def commit(self):
        pass

    def format_bytes(self, num_bytes):
        if num_bytes:
            for unit in ["Bytes", "KiB", "MiB", "GiB", "TiB", "PiB"]:
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
    def open(cls, connection):
        if connection.state == ConnectionState.OPEN:
            logger.debug("Connection is already open, skipping open.")
            return connection

        try:
            connection.handle = create_bigquery_client(connection.credentials)
            connection.state = ConnectionState.OPEN
            return connection

        except Exception as e:
            logger.debug(f"""Got an error when attempting to create a bigquery " "client: '{e}'""")
            connection.handle = None
            connection.state = ConnectionState.FAIL
            raise FailedToConnectError(str(e))

    @classmethod
    def get_table_from_response(cls, resp) -> "agate.Table":
        from dbt_common.clients import agate_helper

        column_names = [field.name for field in resp.schema]
        return agate_helper.table_from_data_flat(resp, column_names)

    def get_labels_from_query_comment(cls):
        if (
            hasattr(cls.profile, "query_comment")
            and cls.profile.query_comment
            and cls.profile.query_comment.job_label
            and cls.query_header
        ):
            query_comment = cls.query_header.comment.query_comment
            return cls._labels_from_query_comment(query_comment)

        return {}

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

        fire_event(SQLQuery(conn_name=conn.name, sql=sql, node_info=get_node_info()))

        labels = self.get_labels_from_query_comment()

        labels["dbt_invocation_id"] = get_invocation_id()

        job_params = {
            "use_legacy_sql": use_legacy_sql,
            "labels": labels,
            "dry_run": dry_run,
        }

        priority = conn.credentials.priority
        if priority == Priority.Batch:
            job_params["priority"] = QueryPriority.BATCH
        else:
            job_params["priority"] = QueryPriority.INTERACTIVE

        maximum_bytes_billed = conn.credentials.maximum_bytes_billed
        if maximum_bytes_billed is not None and maximum_bytes_billed != 0:
            job_params["maximum_bytes_billed"] = maximum_bytes_billed

        with self.exception_handler(sql):
            job_id = self.generate_job_id()

            return self._query_and_results(
                conn,
                sql,
                job_params,
                job_id,
                limit=limit,
            )

    def execute(
        self, sql, auto_begin=False, fetch=None, limit: Optional[int] = None
    ) -> Tuple[BigQueryAdapterResponse, "agate.Table"]:
        sql = self._add_query_comment(sql)
        # auto_begin is ignored on bigquery, and only included for consistency
        query_job, iterator = self.raw_execute(sql, limit=limit)

        if fetch:
            table = self.get_table_from_response(iterator)
        else:
            from dbt_common.clients import agate_helper

            table = agate_helper.empty_table()

        message = "OK"
        code = None
        num_rows = None
        bytes_processed = None
        bytes_billed = None
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
        bytes_billed = query_job.total_bytes_billed
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

        response = BigQueryAdapterResponse(
            _message=message,
            rows_affected=num_rows,
            code=code,
            bytes_processed=bytes_processed,
            bytes_billed=bytes_billed,
            location=location,
            project_id=project_id,
            job_id=job_id,
            slot_ms=slot_ms,
        )

        return response, table

    def dry_run(self, sql: str) -> BigQueryAdapterResponse:
        """Run the given sql statement with the `dry_run` job parameter set.

        This will allow BigQuery to validate the SQL and immediately return job cost
        estimates, which we capture in the BigQueryAdapterResponse. Invalid SQL
        will result in an exception.
        """
        sql = self._add_query_comment(sql)
        query_job, _ = self.raw_execute(sql, dry_run=True)

        # TODO: Factor this repetitive block out into a factory method on
        # BigQueryAdapterResponse
        message = f"Ran dry run query for statement of type {query_job.statement_type}"
        bytes_billed = query_job.total_bytes_billed
        processed_bytes = self.format_bytes(query_job.total_bytes_processed)
        location = query_job.location
        project_id = query_job.project
        job_id = query_job.job_id
        slot_ms = query_job.slot_millis

        return BigQueryAdapterResponse(
            _message=message,
            code="DRY RUN",
            bytes_billed=bytes_billed,
            bytes_processed=processed_bytes,
            location=location,
            project_id=project_id,
            job_id=job_id,
            slot_ms=slot_ms,
        )

    @staticmethod
    def _bq_job_link(location, project_id, job_id) -> str:
        return f"https://console.cloud.google.com/bigquery?project={project_id}&j=bq:{location}:{job_id}&page=queryresults"

    def get_partitions_metadata(self, table):
        def standard_to_legacy(table):
            return table.project + ":" + table.dataset + "." + table.identifier

        legacy_sql = "SELECT * FROM [" + standard_to_legacy(table) + "$__PARTITIONS_SUMMARY__]"

        sql = self._add_query_comment(legacy_sql)
        # auto_begin is ignored on bigquery, and only included for consistency
        _, iterator = self.raw_execute(sql, use_legacy_sql=True)
        return self.get_table_from_response(iterator)

    def copy_bq_table(self, source, destination, write_disposition) -> None:
        conn = self.get_thread_connection()
        client: Client = conn.handle

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

        msg = 'copy table "{}" to "{}"'.format(
            ", ".join(source_ref.path for source_ref in source_ref_array),
            destination_ref.path,
        )
        with self.exception_handler(msg):
            copy_job = client.copy_table(
                source_ref_array,
                destination_ref,
                job_config=CopyJobConfig(write_disposition=write_disposition),
                retry=self._retry.create_reopen_with_deadline(conn),
            )
            copy_job.result(timeout=self._retry.create_job_execution_timeout(fallback=300))

    def write_dataframe_to_table(
        self,
        client: Client,
        file_path: str,
        database: str,
        schema: str,
        identifier: str,
        table_schema: List[SchemaField],
        field_delimiter: str,
        fallback_timeout: Optional[float] = None,
    ) -> None:
        load_config = LoadJobConfig(
            skip_leading_rows=1,
            schema=table_schema,
            field_delimiter=field_delimiter,
        )
        table = self.table_ref(database, schema, identifier)
        self._write_file_to_table(client, file_path, table, load_config, fallback_timeout)

    def write_file_to_table(
        self,
        client: Client,
        file_path: str,
        database: str,
        schema: str,
        identifier: str,
        fallback_timeout: Optional[float] = None,
        **kwargs,
    ) -> None:
        config = kwargs["kwargs"]
        if "schema" in config:
            config["schema"] = json.load(config["schema"])
        load_config = LoadJobConfig(**config)
        table = self.table_ref(database, schema, identifier)
        self._write_file_to_table(client, file_path, table, load_config, fallback_timeout)

    def _write_file_to_table(
        self,
        client: Client,
        file_path: str,
        table: TableReference,
        config: LoadJobConfig,
        fallback_timeout: Optional[float] = None,
    ) -> None:

        with self.exception_handler("LOAD TABLE"):
            with open(file_path, "rb") as f:
                job = client.load_table_from_file(f, table, rewind=True, job_config=config)

        response = job.result(retry=self._retry.create_retry(fallback=fallback_timeout))

        if response.state != "DONE":
            raise DbtRuntimeError("BigQuery Timeout Exceeded")

        elif response.error_result:
            message = "\n".join(error["message"].strip() for error in response.errors)
            raise DbtRuntimeError(message)

    @staticmethod
    def dataset_ref(database, schema):
        return DatasetReference(project=database, dataset_id=schema)

    @staticmethod
    def table_ref(database, schema, table_name):
        dataset_ref = DatasetReference(database, schema)
        return TableReference(dataset_ref, table_name)

    def get_bq_table(self, database, schema, identifier) -> Table:
        """Get a bigquery table for a schema/model."""
        conn = self.get_thread_connection()
        client: Client = conn.handle
        # backwards compatibility: fill in with defaults if not specified
        database = database or conn.credentials.database
        schema = schema or conn.credentials.schema
        return client.get_table(self.table_ref(database, schema, identifier))

    def drop_dataset(self, database, schema) -> None:
        conn = self.get_thread_connection()
        client: Client = conn.handle
        with self.exception_handler("drop dataset"):
            client.delete_dataset(
                dataset=self.dataset_ref(database, schema),
                delete_contents=True,
                not_found_ok=True,
                retry=self._retry.create_reopen_with_deadline(conn),
            )

    def create_dataset(self, database, schema) -> Dataset:
        conn = self.get_thread_connection()
        client: Client = conn.handle
        with self.exception_handler("create dataset"):
            return client.create_dataset(
                dataset=self.dataset_ref(database, schema),
                exists_ok=True,
                retry=self._retry.create_reopen_with_deadline(conn),
            )

    def list_dataset(self, database: str):
        # The database string we get here is potentially quoted.
        # Strip that off for the API call.
        conn = self.get_thread_connection()
        client: Client = conn.handle
        with self.exception_handler("list dataset"):
            # this is similar to how we have to deal with listing tables
            all_datasets = client.list_datasets(
                project=database.strip("`"),
                max_results=10000,
                retry=self._retry.create_reopen_with_deadline(conn),
            )
            return [ds.dataset_id for ds in all_datasets]

    def _query_and_results(
        self,
        conn,
        sql,
        job_params,
        job_id,
        limit: Optional[int] = None,
    ):
        client: Client = conn.handle
        """Query the client and wait for results."""
        # Cannot reuse job_config if destination is set and ddl is used
        query_job = client.query(
            query=sql,
            job_config=QueryJobConfig(**job_params),
            job_id=job_id,  # note, this disables retry since the job_id will have been used
            timeout=self._retry.create_job_creation_timeout(),
        )
        if (
            query_job.location is not None
            and query_job.job_id is not None
            and query_job.project is not None
        ):
            logger.debug(
                self._bq_job_link(query_job.location, query_job.project, query_job.job_id)
            )

        timeout = self._retry.create_job_execution_timeout()
        try:
            iterator = query_job.result(max_results=limit, timeout=timeout)
        except TimeoutError:
            exc = f"Operation did not complete within the designated timeout of {timeout} seconds."
            raise TimeoutError(exc)
        return query_job, iterator

    def _labels_from_query_comment(self, comment: str) -> Dict:
        try:
            comment_labels = json.loads(comment)
        except (TypeError, ValueError):
            return {"query_comment": _sanitize_label(comment)}
        return {
            _sanitize_label(key): _sanitize_label(str(value))
            for key, value in comment_labels.items()
        }


_SANITIZE_LABEL_PATTERN = re.compile(r"[^a-z0-9_-]")

_VALIDATE_LABEL_LENGTH_LIMIT = 63


def _sanitize_label(value: str) -> str:
    """Return a legal value for a BigQuery label."""
    value = value.strip().lower()
    value = _SANITIZE_LABEL_PATTERN.sub("_", value)
    return value[:_VALIDATE_LABEL_LENGTH_LIMIT]
