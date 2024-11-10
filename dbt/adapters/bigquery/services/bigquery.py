from contextlib import contextmanager
import json
from typing import Any, Dict, Generator, Iterator, List, Optional, TYPE_CHECKING, Union

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
    Table,
    TableReference,
    WriteDisposition,
    QueryJob,
)
from google.cloud.exceptions import BadRequest, ClientError, Forbidden, NotFound

from dbt_common.exceptions import DbtDatabaseError, DbtRuntimeError
from dbt.adapters.base.impl import BaseAdapter
from dbt.adapters.base.relation import BaseRelation
from dbt.adapters.contracts.relation import RelationType
from dbt.adapters.events.logging import AdapterLogger

from dbt.adapters.bigquery.column import BigQueryColumn
from dbt.adapters.bigquery.relation import BigQueryRelation

if TYPE_CHECKING:
    import agate


_logger = AdapterLogger("BigQuery")


_DEFAULT_TIMEOUT = 300


def list_datasets(client: Client, database: str, retry: Retry) -> Iterator[Dataset]:
    # The database string we get here is potentially quoted.
    # Strip that off for the API call.
    with _exception_handler():
        return client.list_datasets(database.strip("`"), max_results=10000, retry=retry)


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


def get_dataset(client: Client, relation: BigQueryRelation) -> Optional[Dataset]:
    try:
        return client.get_dataset(dataset_ref(relation))
    except NotFound:
        return None


def delete_dataset(client: Client, relation: BigQueryRelation, retry: Retry) -> None:
    _logger.debug(f'Dropping schema "{relation.database}.{relation.schema}".')

    with _exception_handler():
        client.delete_dataset(
            dataset_ref(relation),
            delete_contents=True,
            not_found_ok=True,
            retry=retry,
        )


def get_table(client: Client, relation: BigQueryRelation, retry: Retry) -> Optional[Table]:
    try:
        return client.get_table(table_ref(relation), retry=retry)
    except NotFound:
        return None


def update_table(
    client: Client, relation: BigQueryRelation, retry: Retry, updates: Dict[str, Any]
) -> Table:
    table = get_table(client, relation, retry)
    for k, v in updates.items():
        setattr(table, k, v)
    client.update_table(table, list(updates.keys()))
    return table


def get_columns(
    client: Client,
    relation: BigQueryRelation,
    retry: Retry,
) -> List[BigQueryColumn]:
    table = get_table(client, relation, retry)
    if table is None:
        return []

    columns = []
    try:
        for column in table.schema:
            # BigQuery returns type labels that are not valid type specifiers
            dtype = BigQueryColumn.translate_type(column.field_type)
            columns.append(BigQueryColumn(column.name, dtype, column.fields, column.mode))
    except ValueError as e:
        _logger.debug("get_columns_in_relation error: {}".format(e))
    return columns


def update_columns(
    client: Client,
    relation: BigQueryRelation,
    retry: Retry,
    columns: Dict[str, Dict[str, Any]],
) -> Table:
    table = get_table(client, relation, retry)
    if table is None:
        raise DbtRuntimeError(f"Table {relation} does not exist!")

    schema = []
    for bq_column in table.schema:
        bq_column_dict = bq_column.to_api_repr()
        new_bq_column_dict = _update_column(bq_column_dict, columns)
        schema.append(SchemaField.from_api_repr(new_bq_column_dict))
    return update_table(client, relation, retry, {"schema": schema})


def _update_column(
    bq_column: Dict[str, Any],
    dbt_columns: Dict[str, Dict[str, Any]],
    parent: Optional[str] = "",
) -> Dict[str, Any]:
    """
    Helper function to recursively traverse the schema of a table in the
    update_column_descriptions function below.

    bq_column_dict should be a dict as obtained by the to_api_repr()
    function of a SchemaField object.
    """
    column_name = bq_column["name"]
    dbt_column_name = f"{parent}.{column_name}" if parent else column_name

    if column_config := dbt_columns.get(dbt_column_name):
        bq_column["description"] = column_config.get("description")
        if bq_column["type"] != "RECORD":
            bq_column["policyTags"] = {"names": column_config.get("policy_tags", [])}

    fields = [
        _update_column(child_bq_column, dbt_columns, parent=dbt_column_name)
        for child_bq_column in bq_column.get("fields", [])
    ]

    bq_column["fields"] = fields

    return bq_column


def copy_table(
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

    with _exception_handler():
        job = client.copy_table(source_refs, destination_ref, job_config=config)
        return job.result(timeout=timeout)


def load_table_from_dataframe(
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

    with _exception_handler():
        with open(file_path, "rb") as f:
            job = client.load_table_from_file(f, destination_ref, rewind=True, job_config=config)
        return job.result(timeout=timeout)


def load_table_from_file(
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

    with _exception_handler():
        with open(file_path, "rb") as f:
            job = client.load_table_from_file(f, destination_ref, rewind=True, job_config=config)
        return job.result(timeout=timeout)


def table_ref(relation: BaseRelation) -> TableReference:
    return TableReference(dataset_ref(relation), relation.identifier)


def dataset_ref(relation: BaseRelation) -> DatasetReference:
    return DatasetReference(relation.database, relation.schema)


def base_relation(table: Optional[Table] = None) -> Optional[BigQueryRelation]:
    if table is None:
        return None

    relation_types = {
        "TABLE": RelationType.Table,
        "VIEW": RelationType.View,
        "MATERIALIZED_VIEW": RelationType.MaterializedView,
        "EXTERNAL": RelationType.External,
    }

    return BigQueryRelation.create(
        database=table.project,
        schema=table.dataset_id,
        identifier=table.table_id,
        quote_policy={"schema": True, "identifier": True},
        type=relation_types.get(table.table_type, RelationType.External),
    )


@contextmanager
def _exception_handler() -> Generator:
    try:
        yield
    except BadRequest as e:
        _database_error(e, "Bad request while running query")
    except Forbidden as e:
        _database_error(e, "Access denied while running query")
    except NotFound as e:
        _database_error(e, "Not found while running query")
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


def _database_error(error: ClientError, message: str) -> None:
    if hasattr(error, "query_job"):
        _logger.error(query_job_url(error.query_job))
    raise DbtDatabaseError(message + "\n".join([item["message"] for item in error.errors]))


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
