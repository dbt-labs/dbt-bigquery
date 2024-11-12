from typing import Any, Dict, List, Optional, TYPE_CHECKING

from google.api_core.retry import Retry
from google.cloud.bigquery import Client, Table, SchemaField
from google.cloud.exceptions import NotFound

from dbt_common.exceptions import DbtRuntimeError

from dbt.adapters.bigquery.column import BigQueryColumn
from dbt.adapters.bigquery.relation import BigQueryRelation
from dbt.adapters.bigquery.services.bigquery._config import DEFAULT_TIMEOUT, logger
from dbt.adapters.bigquery.services.bigquery._table import get_table, update_table
from dbt.adapters.bigquery.services.bigquery._query import execute

if TYPE_CHECKING:
    pass


def get_columns(
    client: Client,
    relation: BigQueryRelation,
    retry: Retry,
) -> List[BigQueryColumn]:
    table = get_table(client, relation, retry)
    if table is None:
        return []

    return [
        BigQueryColumn(
            column.name,
            BigQueryColumn.translate_type(column.field_type),
            column.fields,
            column.mode,
        )
        for column in table.schema
    ]


def update_columns(
    client: Client,
    relation: BigQueryRelation,
    retry: Retry,
    new_columns: Dict[str, Dict[str, Any]],
) -> Table:
    table = get_table(client, relation, retry)
    if table is None:
        raise DbtRuntimeError(f"Table {relation} does not exist!")

    schema = []
    for field in table.schema:
        field_dict = field.to_api_repr()
        new_field_dict = _update_field(field_dict, new_columns)
        schema.append(SchemaField.from_api_repr(new_field_dict))
    return update_table(client, relation, retry, {"schema": schema})


def add_columns(
    client: Client, relation: BigQueryRelation, retry: Retry, new_columns: List[BigQueryColumn]
) -> Table:
    table = get_table(client, relation, retry)
    if table is None:
        raise DbtRuntimeError(f"Table {relation} does not exist!")

    new_fields = [col.column_to_bq_schema() for col in new_columns]
    schema = table.schema + new_fields
    return update_table(client, relation, retry, {"schema": schema})


def _update_field(
    field: Dict[str, Any],
    new_columns: Dict[str, Dict[str, Any]],
    parent: Optional[str] = "",
) -> Dict[str, Any]:
    """
    Helper function to recursively traverse the schema of a table in the
    update_column_descriptions function below.

    bq_column_dict should be a dict as obtained by the to_api_repr()
    function of a SchemaField object.
    """
    field_name = field["name"]
    column_name = f"{parent}.{field_name}" if parent else field_name

    if column := new_columns.get(column_name):
        field["description"] = column.get("description")
        if field["type"] != "RECORD":
            field["policyTags"] = {"names": column.get("policy_tags", [])}

    field["fields"] = [
        _update_field(child_field, new_columns, parent=column_name)
        for child_field in field.get("fields", [])
    ]

    return field


def columns_from_select(
    client: Client,
    sql: str,
    config: Dict[str, Any],
    create_timeout: Optional[float] = DEFAULT_TIMEOUT,
    execute_timeout: Optional[float] = DEFAULT_TIMEOUT,
    job_id: Optional[str] = None,
) -> List[BigQueryColumn]:
    query_job, _ = execute(
        client,
        sql,
        config,
        create_timeout=create_timeout,
        execute_timeout=execute_timeout,
        job_id=job_id,
    )

    try:
        query_table = client.get_table(query_job.destination)
        return columns(query_table.schema)
    except (ValueError, NotFound) as e:
        logger.debug(f"get_columns_in_select_sql error: {e}")
        return []


def columns(fields: List[SchemaField]) -> List[BigQueryColumn]:
    return [BigQueryColumn.create_from_field(field).flatten() for field in fields]
