from dataclasses import dataclass
from typing import Any, Dict, FrozenSet

import agate
from dbt.adapters.relation_configs import RelationConfigChange, RelationResults
from dbt.contracts.graph.nodes import ModelNode

from dbt.adapters.bigquery.relation_configs._base import BigQueryRelationConfigBase


@dataclass(frozen=True, eq=True, unsafe_hash=True)
class BigQueryClusterConfig(BigQueryRelationConfigBase):
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
    def parse_relation_results(cls, relation_results: RelationResults) -> Dict[str, Any]:
        relation_results_entry: agate.Row = cls._get_first_row(relation_results["relation"])  # type: ignore

        field_list = relation_results_entry.get("cluster_by", "")
        config_dict = {"fields": frozenset(field_list.split(","))}

        return config_dict


@dataclass(frozen=True, eq=True, unsafe_hash=True)
class BigQueryClusterConfigChange(RelationConfigChange):
    context: BigQueryClusterConfig

    @property
    def requires_full_refresh(self) -> bool:
        return True
