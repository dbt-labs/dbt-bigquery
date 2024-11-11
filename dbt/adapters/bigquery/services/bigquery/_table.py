import json
from typing import Any, Dict, List, Optional, TYPE_CHECKING, Union

from dbt.adapters.base import BaseAdapter
from google.api_core.retry import Retry
from google.cloud.bigquery import (
    Client,
    CopyJob,
    CopyJobConfig,
    DEFAULT_RETRY,
    LoadJob,
    LoadJobConfig,
    Table,
    TableReference,
    WriteDisposition,
    SchemaField,
)
from google.cloud.exceptions import NotFound

from dbt.adapters.base.relation import BaseRelation
from dbt.adapters.contracts.relation import RelationType

from dbt.adapters.bigquery import BigQueryRelation
from dbt.adapters.bigquery.services.bigquery._config import DEFAULT_TIMEOUT, logger
from dbt.adapters.bigquery.services.bigquery._dataset import dataset_ref
from dbt.adapters.bigquery.services.bigquery._exception import exception_handler

if TYPE_CHECKING:
    import agate


def get_table(
    client: Client, relation: BigQueryRelation, retry: Optional[Retry] = DEFAULT_RETRY
) -> Optional[Table]:
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


def drop_table(
    client: Client, relation: BigQueryRelation, retry: Optional[Retry] = DEFAULT_RETRY
) -> Optional[Table]:
    try:
        return client.delete_table(table_ref(relation), retry=retry)
    except NotFound:
        return None


def copy_table(
    client: Client,
    sources: Union[BigQueryRelation, List[BigQueryRelation]],
    destination: BigQueryRelation,
    mode: str,
    timeout: Optional[float] = DEFAULT_TIMEOUT,
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

    logger.debug(
        'Copying table(s) "{}" to "{}" with disposition: "{}"',
        ", ".join(source_ref.path for source_ref in source_refs),
        destination_ref.path,
        write_disposition,
    )

    with exception_handler():
        job = client.copy_table(source_refs, destination_ref, job_config=config)
        return job.result(timeout=timeout)


def load_table_from_dataframe(
    client: Client,
    file_path: str,
    relation: BigQueryRelation,
    schema: "agate.Table",
    column_override: Dict[str, str],
    field_delimiter: str,
    timeout: Optional[float] = DEFAULT_TIMEOUT,
) -> LoadJob:
    destination_ref = table_ref(relation)

    config = LoadJobConfig(
        skip_leading_rows=1,
        schema=_schema_fields(schema, column_override),
        field_delimiter=field_delimiter,
    )

    with exception_handler():
        with open(file_path, "rb") as f:
            job = client.load_table_from_file(f, destination_ref, rewind=True, job_config=config)
        return job.result(timeout=timeout)


def load_table_from_file(
    client: Client,
    file_path: str,
    relation: BigQueryRelation,
    timeout: Optional[float] = DEFAULT_TIMEOUT,
    **kwargs,
) -> LoadJob:
    destination_ref = table_ref(relation)

    if "schema" in kwargs:
        kwargs["schema"] = json.load(kwargs["schema"])
    config = LoadJobConfig(**kwargs)

    with exception_handler():
        with open(file_path, "rb") as f:
            job = client.load_table_from_file(f, destination_ref, rewind=True, job_config=config)
        return job.result(timeout=timeout)


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


def table_ref(relation: BaseRelation) -> TableReference:
    return TableReference(dataset_ref(relation), relation.identifier)


def _schema_fields(
    agate_table: "agate.Table", column_override: Dict[str, str]
) -> List[SchemaField]:
    """Convert agate.Table with column names to a list of bigquery schemas."""
    bq_schema = []
    for idx, col_name in enumerate(agate_table.column_names):
        inferred_type = BaseAdapter.convert_agate_type(agate_table, idx)
        type_ = column_override.get(col_name, inferred_type)
        bq_schema.append(SchemaField(col_name, type_))
    return bq_schema
