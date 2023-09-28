from dataclasses import dataclass
from typing import List, FrozenSet, Optional, Union

import agate
from dbt.adapters.relation_configs.config_change import RelationConfigChange
from dbt.adapters.relation_configs.config_validation import RelationConfigValidationMixin
from dbt.contracts.graph.nodes import ModelNode

from dbt.adapters.bigquery.relation_configs.base import BigQueryReleationConfigBase


@dataclass(frozen=True, eq=True, unsafe_hash=True)
class BigQueryClusterConfig(BigQueryReleationConfigBase, RelationConfigValidationMixin):
    """
    - cluster_by: A comma-seperated list of of col references to determine cluster.
        - Note: Can contain up to four colms in list.
    """

    cluster_by: Optional[Union[FrozenSet[List[str]], str]] = None

    @classmethod
    def from_dict(cls, config_dict) -> "BigQueryClusterConfig":
        kwargs_dict = {
            "cluster_by": config_dict.get("cluster_by"),
        }
        cluster: "BigQueryClusterConfig" = super().from_dict(kwargs_dict)  # type: ignore
        return cluster

    @classmethod
    def parse_model_node(cls, model_node: ModelNode) -> dict:
        config_dict = {}

        if cluster_by := model_node.config.extra.get("cluster_by"):
            config_dict.update({"cluster_by": cluster_by})

        return config_dict

    @classmethod
    def parse_relation_results(cls, relation_results_entry: agate.Row) -> dict:
        config_dict = {}

        if cluster_by := relation_results_entry.get("cluster_by"):
            config_dict.update({"cluster_by": cluster_by})

        return config_dict


@dataclass(frozen=True, eq=True, unsafe_hash=True)
class BigQueryClusterConfigChange(RelationConfigChange):
    context: BigQueryClusterConfig

    @property
    def requires_full_refresh(self) -> bool:
        return True
