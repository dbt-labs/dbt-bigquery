from typing import Any, Dict, List, Optional, Tuple

from google.cloud.bigquery import Client
from google.cloud.bigquery.dataset import Dataset, DatasetReference
from google.cloud.bigquery.job.copy_ import CopyJob
from google.cloud.bigquery.job.load import LoadJob
from google.cloud.bigquery.table import RowIterator, Table, TableReference

from dbt.adapters.bigquery.column import BigQueryColumn
from dbt.adapters.bigquery.relation import BigQueryRelation
from dbt.adapters.bigquery.services.bigquery._columns import BigQueryColumnsService
from dbt.adapters.bigquery.services.bigquery._dataset import BigQueryDatasetService
from dbt.adapters.bigquery.services.bigquery._options import BigQueryOptionsService
from dbt.adapters.bigquery.services.bigquery._query import (
    BigQueryAdapterResponse,
    BigQueryQueryService,
)
from dbt.adapters.bigquery.services.bigquery._table import BigQueryTableService


class BigQueryService:
    def __init__(self, client: Client) -> None:
        self._client = client
        self._query = BigQueryQueryService(self._client)
        self._dataset = BigQueryDatasetService(self._client)
        self._table = BigQueryTableService(self._client)
        self._columns = BigQueryColumnsService(self._client)
        self._options = BigQueryOptionsService()

    # BigQueryQueryService

    def execute(self, *args, **kwargs) -> Tuple[BigQueryAdapterResponse, RowIterator]:
        return self._query.execute(*args, **kwargs)

    def response(self, *args, **kwargs) -> BigQueryAdapterResponse:
        return self._query.response(*args, **kwargs)

    # BigQueryDatasetService

    def list_schemas(self, *args, **kwargs) -> List[BigQueryRelation]:
        return self._dataset.list_schemas(*args, **kwargs)

    def list_datasets(self, *args, **kwargs) -> List[Dataset]:
        return self._dataset.list(*args, **kwargs)

    def get_dataset(self, *args, **kwargs) -> Optional[Dataset]:
        return self._dataset.get(*args, **kwargs)

    def update_dataset(self, *args, **kwargs) -> Dataset:
        return self._dataset.update(*args, **kwargs)

    def drop_dataset(self, *args, **kwargs) -> None:
        return self._dataset.drop(*args, **kwargs)

    def dataset_exists(self, *args, **kwargs) -> bool:
        return self._dataset.exists(*args, **kwargs)

    def dataset_ref(self, *args, **kwargs) -> DatasetReference:
        return self._dataset.ref(*args, **kwargs)

    # BigQueryTableService

    def list_relations(self, *args, **kwargs) -> List[Optional[BigQueryRelation]]:
        return self._table.list_relations(*args, **kwargs)

    def list_tables(self, *args, **kwargs) -> List[Table]:
        return self._table.list(*args, **kwargs)

    def get_table(self, *args, **kwargs) -> Optional[Table]:
        return self._table.get(*args, **kwargs)

    def update_table(self, *args, **kwargs) -> Table:
        return self._table.update(*args, **kwargs)

    def drop_table(self, *args, **kwargs) -> None:
        return self._table.drop(*args, **kwargs)

    def copy_table(self, *args, **kwargs) -> CopyJob:
        return self._table.copy(*args, **kwargs)

    def load_table_from_dataframe(self, *args, **kwargs) -> LoadJob:
        return self._table.load_from_dataframe(*args, **kwargs)

    def load_table_from_file(self, *args, **kwargs) -> LoadJob:
        return self._table.load_from_file(*args, **kwargs)

    def table_is_replaceable(self, *args, **kwargs) -> bool:
        return self._table.is_replaceable(*args, **kwargs)

    def relation(self, *args, **kwargs) -> Optional[BigQueryRelation]:
        return self._table.relation(*args, **kwargs)

    def table_ref(self, *args, **kwargs) -> TableReference:
        return self._table.ref(*args, **kwargs)

    # BigQueryColumnsService

    def get_columns(self, *args, **kwargs) -> List[BigQueryColumn]:
        return self._columns.get(*args, **kwargs)

    def update_columns(self, *args, **kwargs) -> Table:
        return self._columns.update(*args, **kwargs)

    def add_columns(self, *args, **kwargs) -> Table:
        return self._columns.add(*args, **kwargs)

    def get_columns_from_statement(self, *args, **kwargs) -> List[BigQueryColumn]:
        return self._columns.from_statement(*args, **kwargs)

    def columns(self, *args, **kwargs) -> List[BigQueryColumn]:
        return self._columns.columns(*args, **kwargs)

    # BigQueryOptionsService

    def common_options(self, *args, **kwargs) -> Dict[str, Any]:
        return self._options.common(*args, **kwargs)

    def table_options(self, *args, **kwargs) -> Dict[str, Any]:
        return self._options.table(*args, **kwargs)

    def view_options(self, *args, **kwargs) -> Dict[str, Any]:
        return self._options.view(*args, **kwargs)
