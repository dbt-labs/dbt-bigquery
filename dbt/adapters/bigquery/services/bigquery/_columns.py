from typing import Any, Dict, List, Optional, TYPE_CHECKING

from google.api_core.retry import Retry
from google.cloud.bigquery import DEFAULT_RETRY
from google.cloud.bigquery.client import Client
from google.cloud.bigquery.schema import SchemaField
from google.cloud.bigquery.table import Table
from google.cloud.exceptions import NotFound

from dbt_common.exceptions import DbtRuntimeError

from dbt.adapters.bigquery.column import BigQueryColumn
from dbt.adapters.bigquery.relation import BigQueryRelation
from dbt.adapters.bigquery.services._config import DEFAULT_TIMEOUT, logger
from dbt.adapters.bigquery.services.bigquery._table import BigQueryTableService
from dbt.adapters.bigquery.services.bigquery._query import BigQueryQueryService

if TYPE_CHECKING:
    pass


class BigQueryColumnsService:
    def __init__(self, client: Client) -> None:
        self._client = client
        self._table = BigQueryTableService(client)
        self._query = BigQueryQueryService(client)

    def get(
        self, relation: BigQueryRelation, retry: Optional[Retry] = DEFAULT_RETRY
    ) -> List[BigQueryColumn]:
        table = self._table.get(relation, retry)

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

    def update(
        self,
        relation: BigQueryRelation,
        new_columns: Dict[str, Dict[str, Any]],
        retry: Optional[Retry] = DEFAULT_RETRY,
    ) -> Table:
        table = self._table.get(relation, retry)

        if table is None:
            raise DbtRuntimeError(f"Table {relation} does not exist!")

        schema = []
        for field in table.schema:
            field_dict = field.to_api_repr()
            new_field_dict = _update_field(field_dict, new_columns)
            schema.append(SchemaField.from_api_repr(new_field_dict))
        return self._table.update(relation, {"schema": schema}, retry)

    def add(
        self,
        relation: BigQueryRelation,
        new_columns: List[BigQueryColumn],
        retry: Optional[Retry] = DEFAULT_RETRY,
    ) -> Table:
        table = self._table.get(relation, retry)

        if table is None:
            raise DbtRuntimeError(f"Table {relation} does not exist!")

        new_fields = [col.column_to_bq_schema() for col in new_columns]
        schema = table.schema + new_fields
        return self._table.update(relation, {"schema": schema}, retry)

    def from_statement(
        self,
        statement: str,
        config: Dict[str, Any],
        create_timeout: Optional[float] = DEFAULT_TIMEOUT,
        execute_timeout: Optional[float] = DEFAULT_TIMEOUT,
        job_id: Optional[str] = None,
    ) -> List[BigQueryColumn]:
        query_job, _ = self._query.execute(
            statement,
            config,
            create_timeout=create_timeout,
            execute_timeout=execute_timeout,
            job_id=job_id,
        )

        try:
            query_table = self._client.get_table(query_job.destination)
            return self.columns(query_table.schema)
        except (ValueError, NotFound) as e:
            logger.debug(f"get_columns_in_select_sql error: {e}")
            return []

    @staticmethod
    def columns(fields: List[SchemaField]) -> List[BigQueryColumn]:
        return [BigQueryColumn.create_from_field(field).flatten() for field in fields]


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
