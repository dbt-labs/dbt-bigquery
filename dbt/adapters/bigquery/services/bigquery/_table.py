import json
from typing import Any, Dict, List, Optional, TYPE_CHECKING, Union

from google.api_core.retry import Retry
from google.cloud.bigquery.client import Client
from google.cloud.bigquery.enums import WriteDisposition
from google.cloud.bigquery.job.copy_ import CopyJob, CopyJobConfig
from google.cloud.bigquery.job.load import LoadJob, LoadJobConfig
from google.cloud.bigquery.retry import DEFAULT_RETRY
from google.cloud.bigquery.schema import SchemaField
from google.cloud.bigquery.table import Table, TableReference
from google.cloud.exceptions import NotFound

from dbt.adapters.base.impl import BaseAdapter
from dbt.adapters.base.relation import BaseRelation
from dbt.adapters.contracts.relation import RelationType

from dbt.adapters.bigquery.relation import BigQueryRelation
from dbt.adapters.bigquery.relation_configs import PartitionConfig
from dbt.adapters.bigquery.services._config import DEFAULT_TIMEOUT, logger
from dbt.adapters.bigquery.services.bigquery._dataset import BigQueryDatasetService
from dbt.adapters.bigquery.services.bigquery._exception import exception_handler

if TYPE_CHECKING:
    import agate


class BigQueryTableService:
    def __init__(self, client: Client) -> None:
        self._client = client
        self._dataset = BigQueryDatasetService(client)

    def list_relations(
        self, schema: BigQueryRelation, retry: Optional[Retry] = DEFAULT_RETRY
    ) -> List[Optional[BigQueryRelation]]:
        tables = self.list(schema, retry)
        return [self.relation(table) for table in tables]

    def list(
        self, schema: BigQueryRelation, retry: Optional[Retry] = DEFAULT_RETRY
    ) -> List[Table]:
        dataset = self._dataset.ref(schema)
        try:
            tables = self._client.list_tables(dataset, max_results=100_000, retry=retry)
        except NotFound:
            tables = []
        return list(tables)

    def get(
        self, relation: BigQueryRelation, retry: Optional[Retry] = DEFAULT_RETRY
    ) -> Optional[Table]:
        table = self.ref(relation)
        try:
            return self._client.get_table(table, retry=retry)
        except NotFound:
            return None

    def update(
        self,
        relation: BigQueryRelation,
        updates: Dict[str, Any],
        retry: Optional[Retry] = DEFAULT_RETRY,
    ) -> Table:
        table = self.get(relation, retry)
        for k, v in updates.items():
            setattr(table, k, v)
        self._client.update_table(table, list(updates.keys()))
        return table

    def drop(self, relation: BigQueryRelation, retry: Optional[Retry] = DEFAULT_RETRY) -> None:
        try:
            self._client.delete_table(self.ref(relation), retry=retry)
        except NotFound:
            pass
        return None

    def copy(
        self,
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
            source_refs = [self.ref(src_table) for src_table in sources]
        else:
            source_refs = [self.ref(sources)]

        destination_ref = self.ref(destination)

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
            job = self._client.copy_table(source_refs, destination_ref, job_config=config)
            return job.result(timeout=timeout)

    def load_from_dataframe(
        self,
        file_path: str,
        relation: BigQueryRelation,
        schema: "agate.Table",
        column_override: Dict[str, str],
        field_delimiter: str,
        timeout: Optional[float] = DEFAULT_TIMEOUT,
    ) -> LoadJob:
        destination_ref = self.ref(relation)

        config = LoadJobConfig(
            skip_leading_rows=1,
            schema=_schema_fields(schema, column_override),
            field_delimiter=field_delimiter,
        )

        with exception_handler():
            with open(file_path, "rb") as f:
                job = self._client.load_table_from_file(
                    f, destination_ref, rewind=True, job_config=config
                )
            return job.result(timeout=timeout)

    def load_from_file(
        self,
        file_path: str,
        relation: BigQueryRelation,
        timeout: Optional[float] = DEFAULT_TIMEOUT,
        **kwargs,
    ) -> LoadJob:
        destination_ref = self.ref(relation)

        if "schema" in kwargs:
            kwargs["schema"] = json.load(kwargs["schema"])
        config = LoadJobConfig(**kwargs)

        with exception_handler():
            with open(file_path, "rb") as f:
                job = self._client.load_table_from_file(
                    f, destination_ref, rewind=True, job_config=config
                )
            return job.result(timeout=timeout)

    def is_replaceable(
        self, relation: BigQueryRelation, partition: Optional[PartitionConfig], cluster
    ) -> bool:
        """
        Check if a given partition and clustering column spec for a table
        can replace an existing relation in the database. BigQuery does not
        allow tables to be replaced with another table that has a different
        partitioning spec. This method returns True if the given config spec is
        identical to that of the existing table.
        """
        if not relation:
            return True
        table = self.get(relation)
        if table is None:
            return True
        return all((_partitions_match(table, partition), _clusters_match(table, cluster)))

    @staticmethod
    def relation(table: Optional[Table] = None) -> Optional[BigQueryRelation]:
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

    def ref(self, relation: BaseRelation) -> TableReference:
        return TableReference(self._dataset.ref(relation), relation.identifier)


def _partitions_match(table, conf_partition: Optional[PartitionConfig]) -> bool:
    """
    Check if the actual and configured partitions for a table are a match.
    BigQuery tables can be replaced if:
    - Both tables are not partitioned, OR
    - Both tables are partitioned using the exact same configs

    If there is a mismatch, then the table cannot be replaced directly.
    """
    is_partitioned = table.range_partitioning or table.time_partitioning

    if not is_partitioned and not conf_partition:
        return True
    elif conf_partition and table.time_partitioning is not None:
        table_field = (
            table.time_partitioning.field.lower() if table.time_partitioning.field else None
        )

        table_granularity = table.partitioning_type
        conf_table_field = conf_partition.field
        return (
            table_field == conf_table_field.lower()
            or (conf_partition.time_ingestion_partitioning and table_field is not None)
        ) and table_granularity.lower() == conf_partition.granularity.lower()
    elif conf_partition and table.range_partitioning is not None:
        dest_part = table.range_partitioning
        conf_part = conf_partition.range or {}

        return (
            dest_part.field == conf_partition.field
            and dest_part.range_.start == conf_part.get("start")
            and dest_part.range_.end == conf_part.get("end")
            and dest_part.range_.interval == conf_part.get("interval")
        )
    else:
        return False


def _clusters_match(table, conf_cluster) -> bool:
    """
    Check if the actual and configured clustering columns for a table
    are a match. BigQuery tables can be replaced if clustering columns
    match exactly.
    """
    if isinstance(conf_cluster, str):
        conf_cluster = [conf_cluster]

    return table.clustering_fields == conf_cluster


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
