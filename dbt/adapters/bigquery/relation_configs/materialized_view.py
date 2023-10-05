from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Dict, Optional

import agate
from dbt.adapters.relation_configs import RelationResults
from dbt.contracts.graph.nodes import ModelNode
from dbt.contracts.relation import ComponentName
from dbt.exceptions import DbtRuntimeError

from dbt.adapters.bigquery.relation_configs._base import BigQueryRelationConfigBase
from dbt.adapters.bigquery.relation_configs.auto_refresh import (
    BigQueryAutoRefreshConfig,
    BigQueryAutoRefreshConfigChange,
)
from dbt.adapters.bigquery.relation_configs.partition import (
    PartitionConfig,
    BigQueryPartitionConfigChange,
)
from dbt.adapters.bigquery.relation_configs.cluster import (
    BigQueryClusterConfig,
    BigQueryClusterConfigChange,
)


@dataclass(frozen=True, eq=True, unsafe_hash=True)
class BigQueryMaterializedViewConfig(BigQueryRelationConfigBase):
    """
    This config follow the specs found here:
    https://cloud.google.com/bigquery/docs/reference/standard-sql/data-definition-language#create_materialized_view_statement

    The following parameters are configurable by dbt:
    - materialized_view_name: name of the materialized view
    - schema: dataset name of the materialized view
    - database: project name of the database
    - auto_refresh: object containing refresh scheduling information
    - partition: object containing partition information
    - cluster: object containing cluster information
    - expiration_timestamp: the time when table expires
    - kms_key_name: user defined Cloud KMS encryption key
    - labels: used to organized and group objects
    - description: user description for materialized view
    """

    materialized_view_name: str
    schema_name: str
    database_name: str
    auto_refresh: BigQueryAutoRefreshConfig
    partition: Optional[PartitionConfig] = None
    cluster: Optional[BigQueryClusterConfig] = None
    expiration_timestamp: Optional[datetime] = None
    kms_key_name: Optional[str] = None
    labels: Optional[Dict[str, str]] = None
    description: Optional[str] = None

    @classmethod
    def from_dict(cls, config_dict: Dict[str, Any]) -> "BigQueryMaterializedViewConfig":
        # required
        kwargs_dict: Dict[str, Any] = {
            "materialized_view_name": cls._render_part(
                ComponentName.Identifier, config_dict["materialized_view_name"]
            ),
            "schema_name": cls._render_part(ComponentName.Schema, config_dict["schema_name"]),
            "database_name": cls._render_part(
                ComponentName.Database, config_dict["database_name"]
            ),
            "auto_refresh": BigQueryAutoRefreshConfig.from_dict(config_dict["auto_refresh"]),
        }

        # optional
        if partition := config_dict.get("partition"):
            kwargs_dict.update({"partition": PartitionConfig.parse(partition)})

        if cluster := config_dict.get("cluster"):
            kwargs_dict.update({"cluster": BigQueryClusterConfig.from_dict(cluster)})

        optional_attributes = [
            "expiration_timestamp",
            "kms_key_name",
            "labels",
            "description",
        ]
        optional_attributes_set_by_user = {
            k: v for k, v in config_dict.items() if k in optional_attributes
        }
        kwargs_dict.update(optional_attributes_set_by_user)

        materialized_view: "BigQueryMaterializedViewConfig" = super().from_dict(kwargs_dict)  # type: ignore
        return materialized_view

    @classmethod
    def from_model_node(cls, model_node: ModelNode) -> "BigQueryMaterializedViewConfig":
        materialized_view = super().from_model_node(model_node)
        if isinstance(materialized_view, BigQueryMaterializedViewConfig):
            return materialized_view
        else:
            raise DbtRuntimeError(
                f"An unexpected error occurred in BigQueryMaterializedViewConfig.from_model_node:\n"
                f"    Expected: BigQueryMaterializedViewConfig\n"
                f"    Actual: {materialized_view}"
            )

    @classmethod
    def parse_model_node(cls, model_node: ModelNode) -> Dict[str, Any]:
        config_dict = {
            "materialized_view_name": model_node.identifier,
            "schema_name": model_node.schema,
            "database_name": model_node.database,
            "auto_refresh": BigQueryAutoRefreshConfig.parse_model_node(model_node),
        }

        # optional
        if "partition_by" in model_node.config:
            config_dict.update({"partition": PartitionConfig.parse_model_node(model_node)})

        if "cluster_by" in model_node.config:
            config_dict.update({"cluster": BigQueryClusterConfig.parse_model_node(model_node)})

        if hours_to_expiration := model_node.config.extra.get("hours_to_expiration"):
            config_dict.update(
                {"expiration_timestamp": datetime.now() + timedelta(hours=hours_to_expiration)}
            )

        if kms_key_name := model_node.config.extra.get("kms_key_name"):
            config_dict.update({"kms_key_name": kms_key_name})

        if labels := model_node.config.extra.get("labels"):
            config_dict.update({"labels": labels})

        if description := model_node.config.extra.get("description"):
            if model_node.config.persist_docs:
                config_dict.update({"description": description})

        return config_dict

    @classmethod
    def parse_relation_results(cls, relation_results: RelationResults) -> Dict[str, Any]:
        materialized_view_config: agate.Table = relation_results.get("materialized_view")  # type: ignore
        materialized_view: agate.Row = cls._get_first_row(materialized_view_config)
        options_config: agate.Table = relation_results.get("options")  # type: ignore
        options = {
            option.get("option_name"): option.get("option_value") for option in options_config.rows
        }

        config_dict = {
            "materialized_view_name": materialized_view.get("table_name"),
            "schema_name": materialized_view.get("table_schema"),
            "database_name": materialized_view.get("table_catalog"),
            "auto_refresh": BigQueryAutoRefreshConfig.parse_relation_results(relation_results),
        }

        # optional
        partition_by: agate.Table = relation_results.get("partition_by")  # type: ignore
        if len(partition_by) > 0:
            config_dict.update(
                {"partition": PartitionConfig.parse_relation_results(partition_by[0])}
            )

        cluster_by: agate.Table = relation_results.get("cluster_by")  # type: ignore
        if len(cluster_by) > 0:
            config_dict.update(
                {"cluster": BigQueryClusterConfig.parse_relation_results(cluster_by)}
            )

        config_dict.update(
            {
                "expiration_timestamp": options.get("expiration_timestamp"),
                "kms_key_name": options.get("kms_key_name"),
                "labels": options.get("labels"),
                "description": options.get("description"),
            }
        )

        return config_dict


@dataclass
class BigQueryMaterializedViewConfigChangeset:
    auto_refresh: Optional[BigQueryAutoRefreshConfigChange] = None
    partition: Optional[BigQueryPartitionConfigChange] = None
    cluster: Optional[BigQueryClusterConfigChange] = None
    expiration_timestamp: Optional[datetime] = None
    kms_key_name: Optional[str] = None
    labels: Optional[Dict[str, str]] = None
    description: Optional[str] = None

    @property
    def requires_full_refresh(self) -> bool:
        return any(
            {
                self.auto_refresh.requires_full_refresh if self.auto_refresh else False,
                self.partition.requires_full_refresh if self.partition else False,
                self.cluster.requires_full_refresh if self.cluster else False,
            }
        )

    @property
    def has_changes(self) -> bool:
        return any(
            {
                self.partition if self.partition else False,
                self.cluster if self.cluster else False,
                self.auto_refresh if self.auto_refresh else False,
            }
        )
