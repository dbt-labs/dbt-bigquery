from dataclasses import dataclass
from typing import Any, Dict, FrozenSet, Optional

import agate

from dbt.adapters.bigquery.relation_configs.base import BigQueryReleationConfigBase
from dbt.adapters.relation_configs.config_change import RelationConfigChange
from dbt.adapters.relation_configs.config_validation import RelationConfigValidationMixin
from dbt.contracts.graph.nodes import ModelNode


@dataclass(frozen=True, eq=True, unsafe_hash=True)
class BigQueryPartitionConfig(BigQueryReleationConfigBase, RelationConfigValidationMixin):
    """
    This config dictionary is comprised of 2table options all centered around partitioning
    - partition_by: Expression to describe how to partition materialized view.
        - Note: Must be partitioned in the same was as base table is partitioned.
    - partition_expiration_days: The default lifetime, in days, of all partitions in a
        partitioned table
    """

    partition_by: Optional[FrozenSet[Dict[str, Any]]] = None
    partition_expiration_days: Optional[int] = None

    @classmethod
    def from_dict(cls, config_dict) -> "BigQueryPartitionConfig":
        kwargs_dict = {
            "partition_by": config_dict.get("partition_by"),
            "partition_expiration_days": config_dict.geet("partition_expiration_days"),
        }

        partition: "BigQueryPartitionConfig" = super().from_dict(kwargs_dict)  # type: ignore
        return partition

    @classmethod
    def parse_model_node(cls, model_node: ModelNode) -> dict:
        config_dict = {}

        if partition_by := model_node.config.extra.get("partition_by"):
            config_dict.update({"partition_by": partition_by})

        if partition_expiration_days := model_node.config.extra.get("partition_expiration_days"):
            config_dict.update({"partition_expiration_days": partition_expiration_days})

        return config_dict

    @classmethod
    def parse_relation_results(cls, relation_results_entry: agate.Row) -> dict:
        config_dict = {}

        if partition_by := relation_results_entry.get("partition_by"):
            config_dict.update({"partition_by": partition_by})

        if partition_expiration_days := relation_results_entry.get("partition_expiration_days"):
            config_dict.update({"partition_expiration_days": partition_expiration_days})

        return config_dict


@dataclass(frozen=True, eq=True, unsafe_hash=True)
class BigQueryPartitionConfigChange(RelationConfigChange):
    context: BigQueryPartitionConfig

    @property
    def requires_full_refresh(self) -> bool:
        return True
