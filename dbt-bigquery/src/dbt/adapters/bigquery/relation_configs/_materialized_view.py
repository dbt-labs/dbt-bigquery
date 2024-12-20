from dataclasses import dataclass
from typing import Any, Dict, Optional

from dbt.adapters.contracts.relation import (
    RelationConfig,
    ComponentName,
)
from google.cloud.bigquery import Table as BigQueryTable

from dbt.adapters.bigquery.relation_configs._base import BigQueryBaseRelationConfig
from dbt.adapters.bigquery.relation_configs._options import (
    BigQueryOptionsConfig,
    BigQueryOptionsConfigChange,
)
from dbt.adapters.bigquery.relation_configs._partition import (
    BigQueryPartitionConfigChange,
    PartitionConfig,
)
from dbt.adapters.bigquery.relation_configs._cluster import (
    BigQueryClusterConfig,
    BigQueryClusterConfigChange,
)


@dataclass(frozen=True, eq=True, unsafe_hash=True)
class BigQueryMaterializedViewConfig(BigQueryBaseRelationConfig):
    """
    This config follow the specs found here:
    https://cloud.google.com/bigquery/docs/reference/standard-sql/data-definition-language#create_materialized_view_statement

    The following parameters are configurable by dbt:
    - table_id: name of the materialized view
    - dataset_id: dataset name of the materialized view
    - project_id: project name of the database
    - options: options that get set in `SET OPTIONS()` clause
    - partition: object containing partition information
    - cluster: object containing cluster information
    """

    table_id: str
    dataset_id: str
    project_id: str
    options: BigQueryOptionsConfig
    partition: Optional[PartitionConfig] = None
    cluster: Optional[BigQueryClusterConfig] = None

    @classmethod
    def from_dict(cls, config_dict: Dict[str, Any]) -> "BigQueryMaterializedViewConfig":
        # required
        kwargs_dict: Dict[str, Any] = {
            "table_id": cls._render_part(ComponentName.Identifier, config_dict["table_id"]),
            "dataset_id": cls._render_part(ComponentName.Schema, config_dict["dataset_id"]),
            "project_id": cls._render_part(ComponentName.Database, config_dict["project_id"]),
            "options": BigQueryOptionsConfig.from_dict(config_dict["options"]),
        }

        # optional
        if partition := config_dict.get("partition"):
            kwargs_dict.update({"partition": PartitionConfig.parse(partition)})

        if cluster := config_dict.get("cluster"):
            kwargs_dict.update({"cluster": BigQueryClusterConfig.from_dict(cluster)})

        materialized_view: "BigQueryMaterializedViewConfig" = super().from_dict(kwargs_dict)
        return materialized_view

    @classmethod
    def parse_relation_config(cls, relation_config: RelationConfig) -> Dict[str, Any]:
        config_dict = {
            "table_id": relation_config.identifier,
            "dataset_id": relation_config.schema,
            "project_id": relation_config.database,
            # despite this being a foreign object, there will always be options because of defaults
            "options": BigQueryOptionsConfig.parse_relation_config(relation_config),
        }

        # optional
        if relation_config.config and "partition_by" in relation_config.config:
            config_dict.update({"partition": PartitionConfig.parse_model_node(relation_config)})

        if relation_config.config and "cluster_by" in relation_config.config:
            config_dict.update(
                {"cluster": BigQueryClusterConfig.parse_relation_config(relation_config)}
            )

        return config_dict

    @classmethod
    def parse_bq_table(cls, table: BigQueryTable) -> Dict[str, Any]:
        config_dict = {
            "table_id": table.table_id,
            "dataset_id": table.dataset_id,
            "project_id": table.project,
            # despite this being a foreign object, there will always be options because of defaults
            "options": BigQueryOptionsConfig.parse_bq_table(table),
        }

        # optional
        if table.time_partitioning or table.range_partitioning:
            config_dict.update({"partition": PartitionConfig.parse_bq_table(table)})

        if table.clustering_fields:
            config_dict.update({"cluster": BigQueryClusterConfig.parse_bq_table(table)})

        return config_dict


@dataclass
class BigQueryMaterializedViewConfigChangeset:
    options: Optional[BigQueryOptionsConfigChange] = None
    partition: Optional[BigQueryPartitionConfigChange] = None
    cluster: Optional[BigQueryClusterConfigChange] = None

    @property
    def requires_full_refresh(self) -> bool:
        return any(
            {
                self.options.requires_full_refresh if self.options else False,
                self.partition.requires_full_refresh if self.partition else False,
                self.cluster.requires_full_refresh if self.cluster else False,
            }
        )

    @property
    def has_changes(self) -> bool:
        return any(
            {
                self.options if self.options else False,
                self.partition if self.partition else False,
                self.cluster if self.cluster else False,
            }
        )
