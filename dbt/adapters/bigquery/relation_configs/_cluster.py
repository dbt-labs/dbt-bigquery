from dataclasses import dataclass
from typing import Any, Dict, FrozenSet, Optional

from dbt.adapters.relation_configs import RelationConfigChange
from dbt.contracts.graph.nodes import ModelNode
from google.cloud.bigquery import Table as BigQueryTable

from dbt.adapters.bigquery.relation_configs._base import BigQueryBaseRelationConfig


@dataclass(frozen=True, eq=True, unsafe_hash=True)
class BigQueryClusterConfig(BigQueryBaseRelationConfig):
    """
    This config manages table options supporting clustering. See the following for more information:
        - https://docs.getdbt.com/reference/resource-configs/bigquery-configs#using-table-partitioning-and-clustering
        - https://cloud.google.com/bigquery/docs/reference/standard-sql/data-definition-language#clustering_column_list

    - fields: set of columns to cluster on
        - Note: can contain up to four columns
    """

    fields: FrozenSet[str]

    @classmethod
    def from_dict(cls, config_dict: Dict[str, Any]) -> "BigQueryClusterConfig":
        kwargs_dict = {"fields": config_dict.get("fields")}
        cluster: "BigQueryClusterConfig" = super().from_dict(kwargs_dict)  # type: ignore
        return cluster

    @classmethod
    def parse_model_node(cls, model_node: ModelNode) -> Dict[str, Any]:
        config_dict = {}

        if cluster_by := model_node.config.extra.get("cluster_by"):
            # users may input a single field as a string
            if isinstance(cluster_by, str):
                cluster_by = [cluster_by]
            config_dict.update({"fields": frozenset(cluster_by)})

        return config_dict

    @classmethod
    def parse_bq_table(cls, table: BigQueryTable) -> Dict[str, Any]:  # type: ignore
        config_dict = {"fields": frozenset(table.clustering_fields)}
        return config_dict


@dataclass(frozen=True, eq=True, unsafe_hash=True)
class BigQueryClusterConfigChange(RelationConfigChange):
    context: Optional[BigQueryClusterConfig]

    @property
    def requires_full_refresh(self) -> bool:
        return True
