from contextlib import contextmanager
import json
from typing import Dict, Generator, Iterator, List, Optional, TYPE_CHECKING, Union

from google.api_core.retry import Retry
from google.auth.exceptions import RefreshError
from google.cloud.bigquery import (
    Client,
    CopyJob,
    CopyJobConfig,
    Dataset,
    DatasetReference,
    LoadJob,
    LoadJobConfig,
    SchemaField,
    TableReference,
    WriteDisposition,
    QueryJob,
)
from google.cloud.exceptions import BadRequest, ClientError, Forbidden, NotFound

from dbt_common.exceptions import DbtDatabaseError, DbtRuntimeError
from dbt.adapters.base.impl import BaseAdapter
from dbt.adapters.events.logging import AdapterLogger

from dbt.adapters.bigquery.relation import BigQueryRelation

if TYPE_CHECKING:
    import agate


_logger = AdapterLogger("BigQuery")


_DEFAULT_TIMEOUT = 300


class BigQueryService:
    def __init__(self) -> None:
        pass

    def list_datasets(self, client: Client, database: str, retry: Retry) -> Iterator[Dataset]:
        # The database string we get here is potentially quoted.
        # Strip that off for the API call.
        with self._exception_handler():
            return client.list_datasets(database.strip("`"), max_results=10000, retry=retry)

    @staticmethod
    def dataset_exists(client: Client, relation: BigQueryRelation) -> bool:
        """
        Determine whether a dataset exists.

        This tries to do something with the dataset and checks for an exception.
        If the dataset doesn't exist it will 404.
        We have to do it this way to handle underscore-prefixed datasets,
        which don't appear in the information_schema.schemata view nor the
        list_datasets method.

        Args:
            client: a client with view privileges on the dataset
            relation: the dataset that we're checking

        Returns:
            True if the dataset exists, False otherwise
        """
        dataset = dataset_ref(relation)
        try:
            next(iter(client.list_tables(dataset, max_results=1)))
        except StopIteration:
            pass
        except NotFound:
            # the schema does not exist
            return False
        return True

    def delete_dataset(self, client: Client, relation: BigQueryRelation, retry: Retry) -> None:
        _logger.debug(f'Dropping schema "{relation.database}.{relation.schema}".')

        with self._exception_handler():
            client.delete_dataset(
                dataset_ref(relation),
                delete_contents=True,
                not_found_ok=True,
                retry=retry,
            )

    def copy_table(
        self,
        client: Client,
        sources: Union[BigQueryRelation, List[BigQueryRelation]],
        destination: BigQueryRelation,
        mode: str,
        timeout: Optional[float] = _DEFAULT_TIMEOUT,
    ) -> CopyJob:
        # -------------------------------------------------------------------------------
        #  BigQuery allows to use copy API using two different formats:
        #  1. client.copy_table(source_table_id, destination_table_id)
        #     where source_table_id = "your-project.source_dataset.source_table"
        #  2. client.copy_table(source_table_ids, destination_table_id)
        #     where source_table_ids = ["your-project.your_dataset.your_table_name", ...]
        #  Let's use uniform function call and always pass list there
        # -------------------------------------------------------------------------------
        if isinstance(sources, list):
            source_refs = [table_ref(src_table) for src_table in sources]
        else:
            source_refs = [table_ref(sources)]

        destination_ref = table_ref(destination)

        write_disposition = {
            "incremental": WriteDisposition.WRITE_APPEND,
            "table": WriteDisposition.WRITE_TRUNCATE,
        }.get(mode, WriteDisposition.WRITE_TRUNCATE)

        config = CopyJobConfig(write_disposition=write_disposition)

        _logger.debug(
            'Copying table(s) "{}" to "{}" with disposition: "{}"',
            ", ".join(source_ref.path for source_ref in source_refs),
            destination_ref.path,
            write_disposition,
        )

        with self._exception_handler():
            job = client.copy_table(source_refs, destination_ref, job_config=config)
            return job.result(timeout=timeout)

    def load_table_from_dataframe(
        self,
        client: Client,
        file_path: str,
        relation: BigQueryRelation,
        schema: "agate.Table",
        column_override: Dict[str, str],
        field_delimiter: str,
        timeout: Optional[float] = _DEFAULT_TIMEOUT,
    ) -> LoadJob:
        destination_ref = table_ref(relation)

        config = LoadJobConfig(
            skip_leading_rows=1,
            schema=schema_fields(schema, column_override),
            field_delimiter=field_delimiter,
        )

        with self._exception_handler():
            with open(file_path, "rb") as f:
                job = client.load_table_from_file(
                    f, destination_ref, rewind=True, job_config=config
                )
            return job.result(timeout=timeout)

    def load_table_from_file(
        self,
        client: Client,
        file_path: str,
        relation: BigQueryRelation,
        timeout: Optional[float] = _DEFAULT_TIMEOUT,
        **kwargs,
    ) -> LoadJob:
        destination_ref = table_ref(relation)

        if "schema" in kwargs:
            kwargs["schema"] = json.load(kwargs["schema"])
        config = LoadJobConfig(**kwargs)

        with self._exception_handler():
            with open(file_path, "rb") as f:
                job = client.load_table_from_file(
                    f, destination_ref, rewind=True, job_config=config
                )
            return job.result(timeout=timeout)

    @contextmanager
    def _exception_handler(self) -> Generator:
        try:
            yield
        except BadRequest as e:
            self._database_error(e, "Bad request while running query")
        except Forbidden as e:
            self._database_error(e, "Access denied while running query")
        except NotFound as e:
            self._database_error(e, "Not found while running query")
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

    @staticmethod
    def _database_error(error: ClientError, message: str) -> None:
        if hasattr(error, "query_job"):
            _logger.error(query_job_url(error.query_job))
        raise DbtDatabaseError(message + "\n".join([item["message"] for item in error.errors]))


def table_ref(relation: BigQueryRelation) -> TableReference:
    return TableReference(dataset_ref(relation), relation.identifier)


def dataset_ref(relation: BigQueryRelation) -> DatasetReference:
    return DatasetReference(relation.database, relation.schema)


def query_job_url(query_job: QueryJob) -> str:
    return f"https://console.cloud.google.com/bigquery?project={query_job.project}&j=bq:{query_job.location}:{query_job.job_id}&page=queryresults"


def schema_fields(
    agate_table: "agate.Table", column_override: Dict[str, str]
) -> List[SchemaField]:
    """Convert agate.Table with column names to a list of bigquery schemas."""
    bq_schema = []
    for idx, col_name in enumerate(agate_table.column_names):
        inferred_type = BaseAdapter.convert_agate_type(agate_table, idx)
        type_ = column_override.get(col_name, inferred_type)
        bq_schema.append(SchemaField(col_name, type_))
    return bq_schema
