from collections import namedtuple
from dataclasses import dataclass
from typing import Any, Dict, Optional

import agate

from dbt.adapters.relation_configs import RelationConfigChange
from dbt.contracts.graph.nodes import ModelNode
from dbt.dataclass_schema import StrEnum

from dbt.adapters.bigquery.relation_configs._base import BigQueryRelationConfigBase


class PartitionDataType(StrEnum):
    TIMESTAMP = "timestamp"
    DATE = "date"
    DATETIME = "datetime"
    INT64 = "int64"


class PartitionGranularity(StrEnum):
    HOUR = "hour"
    DAY = "day"
    MONTH = "month"
    YEAR = "year"


PartitionRange = namedtuple("PartitionRange", ["start", "end", "interval"])


@dataclass(frozen=True, eq=True, unsafe_hash=True)
class BigQueryPartitionConfig(BigQueryRelationConfigBase):
    """
    This config manages table options supporting partitioning. See the following for more information:
        - https://docs.getdbt.com/reference/resource-configs/bigquery-configs#using-table-partitioning-and-clustering
        - https://cloud.google.com/bigquery/docs/reference/standard-sql/data-definition-language#partition_expression

    - field: field to partition on
        - Note: Must be partitioned in the same way as base table
    - data_type: data type of `field`
    - granularity: size of the buckets for non-int64 `data_type`
    - range: size of the buckets for int64 `data_type`
    - time_ingestion_partitioning: supports partitioning by row creation time
    """

    field: str
    data_type: PartitionDataType
    granularity: Optional[PartitionGranularity] = None
    range: Optional[PartitionRange] = None
    time_ingestion_partitioning: Optional[bool] = False

    @classmethod
    def from_dict(cls, config_dict: Dict[str, Any]) -> "BigQueryPartitionConfig":
        # required
        kwargs_dict = {
            "field": config_dict.get("field"),
            "data_type": config_dict.get("data_type"),
        }

        # optional
        if granularity := config_dict.get("granularity"):
            config_dict.update({"granularity": granularity})
        if partition_range := config_dict.get("range"):
            config_dict.update({"range": PartitionRange(**partition_range)})

        partition: "BigQueryPartitionConfig" = super().from_dict(kwargs_dict)  # type: ignore
        return partition

    @classmethod
    def parse_model_node(cls, model_node: ModelNode) -> Dict[str, Any]:
        partition_by = model_node.config.extra.get("partition_by", {})

        config_dict = {
            "field": partition_by.get("field"),
            "data_type": partition_by.get("data_type"),
            "granularity": partition_by.get("granularity"),
            "range": partition_by.get("range", {}),
        }

        return config_dict

    @classmethod
    def parse_relation_results(cls, relation_results_entry: agate.Row) -> Dict[str, Any]:  # type: ignore
        config_dict = {
            "field": relation_results_entry.get("partition_field"),
            "data_type": relation_results_entry.get("partition_data_type"),
            "granularity": relation_results_entry.get("partition_granularity"),
        }

        # combine range fields into dictionary, like the model config
        range_dict = {
            "start": relation_results_entry.get("partition_start"),
            "end": relation_results_entry.get("partition_end"),
            "interval": relation_results_entry.get("partition_interval"),
        }
        config_dict.update({"range": range_dict})

        return config_dict


@dataclass(frozen=True, eq=True, unsafe_hash=True)
class BigQueryPartitionConfigChange(RelationConfigChange):
    context: BigQueryPartitionConfig

    @property
    def requires_full_refresh(self) -> bool:
        return True
