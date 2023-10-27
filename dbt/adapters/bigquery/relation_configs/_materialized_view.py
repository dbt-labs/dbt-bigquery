from dataclasses import dataclass
from typing import Any, Dict, Optional
from typing_extensions import Self

from dbt.adapters.relation_configs import MaterializedViewRelationConfig
from dbt.contracts.graph.nodes import ParsedNode
from dbt.contracts.relation import ComponentName
from google.cloud.bigquery import Table as BigQueryTable

from dbt.adapters.bigquery.relation_configs._cluster import (
    BigQueryClusterConfig,
    BigQueryClusterConfigChange,
)
from dbt.adapters.bigquery.relation_configs._options import (
    BigQueryOptionsConfig,
    BigQueryOptionsConfigChange,
)
from dbt.adapters.bigquery.relation_configs._partition import (
    BigQueryPartitionConfigChange,
    PartitionConfig,
)
from dbt.adapters.bigquery.relation_configs._policies import render_part


@dataclass(frozen=True, eq=True, unsafe_hash=True)
class BigQueryMaterializedViewConfig(MaterializedViewRelationConfig):
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

    @property
    def auto_refresh(self):
        return self.options.enable_refresh

    @property
    def fully_qualified_path(self) -> str:
        return ".".join(
            render_part(component, part)
            for component, part in {
                ComponentName.Database: self.project_id,
                ComponentName.Schema: self.dataset_id,
                ComponentName.Identifier: self.table_id,
            }.items()
        )

    @classmethod
    def from_dict(cls, config_dict: Dict[str, Any]) -> Self:
        # required
        kwargs_dict: Dict[str, Any] = {
            "table_id": config_dict["table_id"],
            "dataset_id": config_dict["dataset_id"],
            "project_id": config_dict["project_id"],
            "options": BigQueryOptionsConfig.from_dict(config_dict["options"]),
        }

        # optional
        if partition := config_dict.get("partition"):
            kwargs_dict.update({"partition": PartitionConfig.parse(partition)})

        if cluster := config_dict.get("cluster"):
            kwargs_dict.update({"cluster": BigQueryClusterConfig.from_dict(cluster)})

        return super().from_dict(kwargs_dict)  # type: ignore

    @classmethod
    def parse_node(cls, node: ParsedNode) -> Dict[str, Any]:
        config_dict = {
            "table_id": node.identifier,
            "dataset_id": node.schema,
            "project_id": node.database,
            # despite this being a foreign object, there will always be options because of defaults
            "options": BigQueryOptionsConfig.parse_node(node),
        }

        # optional
        if "partition_by" in node.config:
            config_dict.update({"partition": PartitionConfig.parse_node(node)})

        if "cluster_by" in node.config:
            config_dict.update({"cluster": BigQueryClusterConfig.parse_node(node)})

        return config_dict

    @classmethod
    def parse_api_results(cls, table: BigQueryTable) -> Dict[str, Any]:
        config_dict = {
            "table_id": table.table_id,
            "dataset_id": table.dataset_id,
            "project_id": table.project,
            # despite this being a foreign object, there will always be options because of defaults
            "options": BigQueryOptionsConfig.parse_api_results(table),
        }

        # optional
        if table.time_partitioning or table.range_partitioning:
            config_dict.update({"partition": PartitionConfig.parse_api_results(table)})

        if table.clustering_fields:
            config_dict.update({"cluster": BigQueryClusterConfig.parse_api_results(table)})

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
