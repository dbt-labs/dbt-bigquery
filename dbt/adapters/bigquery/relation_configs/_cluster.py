from dataclasses import dataclass
from typing import Any, Dict, FrozenSet, Optional
from typing_extensions import Self

from dbt.adapters.relation_configs import RelationConfigBase, RelationConfigChange
from dbt.contracts.graph.nodes import ParsedNode
from google.cloud.bigquery import Table as BigQueryTable


@dataclass(frozen=True, eq=True, unsafe_hash=True)
class BigQueryClusterConfig(RelationConfigBase):
    """
    This config manages table options supporting clustering. See the following for more information:
        - https://docs.getdbt.com/reference/resource-configs/bigquery-configs#using-table-partitioning-and-clustering
        - https://cloud.google.com/bigquery/docs/reference/standard-sql/data-definition-language#clustering_column_list

    - fields: set of columns to cluster on
        - Note: can contain up to four columns
    """

    fields: FrozenSet[str]

    @classmethod
    def from_dict(cls, config_dict: Dict[str, Any]) -> Self:
        kwargs_dict = {"fields": config_dict.get("fields")}
        return super().from_dict(kwargs_dict)  # type: ignore

    @classmethod
    def parse_node(cls, node: ParsedNode) -> Dict[str, Any]:
        config_dict = {}

        if cluster_by := node.config.extra.get("cluster_by"):
            # users may input a single field as a string
            if isinstance(cluster_by, str):
                cluster_by = [cluster_by]
            config_dict.update({"fields": frozenset(cluster_by)})

        return config_dict

    @classmethod
    def parse_api_results(cls, table: BigQueryTable) -> Dict[str, Any]:  # type: ignore
        config_dict = {"fields": frozenset(table.clustering_fields)}
        return config_dict


@dataclass(frozen=True, eq=True, unsafe_hash=True)
class BigQueryClusterConfigChange(RelationConfigChange):
    context: Optional[BigQueryClusterConfig]

    @property
    def requires_full_refresh(self) -> bool:
        return True
