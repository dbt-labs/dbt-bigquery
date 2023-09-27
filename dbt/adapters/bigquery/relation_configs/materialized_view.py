from dataclasses import dataclass
from typing import Dict, Optional

import agate
from dbt.exceptions import DbtRuntimeError
from dbt.adapters.relation_configs.config_base import RelationResults
from dbt.adapters.relation_configs.config_validation import RelationConfigValidationMixin
from dbt.contracts.graph.nodes import ModelNode
from dbt.contracts.relation import ComponentName
from dbt.adapters.bigquery.relation_configs.base import BigQueryReleationConfigBase
from dbt.adapters.bigquery.relation_configs.auto_refresh import (
    BigQueryAutoRefreshConfig,
    BigQueryAutoRefreshConfigChange,
)
from dbt.adapters.bigquery.relation_configs.partition import (
    BigQueryPartitionConfig,
    BigQueryPartitionConfigChange,
)
from dbt.adapters.bigquery.relation_configs.cluster import (
    BigQueryClusterConfig,
    BigQueryClusterConfigChange,
)


@dataclass(frozen=True, eq=True, unsafe_hash=True)
class BigQueryMaterializedViewConfig(BigQueryReleationConfigBase, RelationConfigValidationMixin):
    """
    This config follow the specs found here:
    https://cloud.google.com/bigquery/docs/reference/standard-sql/data-definition-language#create_materialized_view_statement

    The following parameters are configurable by dbt:
    - materialized_view_name: Name of the materialized view
    - schema: Dataset name of the materialized view
    - database: Project name of the database
    - hours_to_expiration: The time when table expires.
        - Note: If not set table persists
    - kms_key_name: user defined Cloud KMS encryption key.
    - labels: used to organized and group table
        - Note on usage can be found

    There are currently no non-configurable parameters.
    """

    materialized_view_name: str
    schema_name: str
    database_name: str
    cluster: BigQueryClusterConfig = BigQueryClusterConfig()
    partition: BigQueryPartitionConfig = BigQueryPartitionConfig()
    auto_refresh: BigQueryAutoRefreshConfig = BigQueryAutoRefreshConfig()
    hours_to_expiration: Optional[int] = None
    kms_key_name: Optional[str] = None
    labels: Optional[Dict[str, str]] = None

    @classmethod
    def from_dict(cls, config_dict) -> "BigQueryMaterializedViewConfig":
        kwargs_dict = {
            "materialized_view_name": cls._render_part(
                ComponentName.Identifier, config_dict.get("materialized_view_name")
            ),
            "schema_name": cls._render_part(ComponentName.Schema, config_dict.get("schema_name")),
            "database_name": cls._render_part(
                ComponentName.Database, config_dict.get("database_name")
            ),
            "hours_to_expiration": config_dict.get("hours_to_expiration"),
            "kms_key_name": config_dict.get("kms_key_name"),
            "labels": config_dict.get("labels"),
        }

        if auto_refresh := config_dict.get("auto_refresh"):
            kwargs_dict.update({"auto_refresh": BigQueryAutoRefreshConfig.from_dict(auto_refresh)})

        if partition := config_dict.get("partition"):
            kwargs_dict.update({"partition": BigQueryPartitionConfig.from_dict(partition)})

        if cluster := config_dict.get("cluster"):
            kwargs_dict.update({"cluster": BigQueryClusterConfig.from_dict(cluster)})

        materialized_view: "BigQueryMaterializedViewConfig" = super().from_dict(kwargs_dict)  # type: ignore
        return materialized_view

    @classmethod
    def parse_model_node(cls, model_node: ModelNode) -> dict:
        config_dict = {
            "materialized_view_name": model_node.identifier,
            "schema_name": model_node.schema,
            "database_name": model_node.database,
            "hours_to_expiration": model_node.config.extra.get("hours_to_expiration"),
            "kms_key_name": model_node.config.extra.get("kms_key_name"),
            "labels": model_node.config.extra.get("labels"),
        }

        if model_node.config.get("auto_refresh"):
            config_dict.update(
                {"auto_refresh": BigQueryAutoRefreshConfig.parse_model_node(model_node)}
            )

        if model_node.config.get("partition"):
            config_dict.update({"partition": BigQueryPartitionConfig.parse_model_node(model_node)})

        if model_node.config.get("cluster"):
            config_dict.update({"cluster": BigQueryClusterConfig.parse_model_node(model_node)})

        return config_dict

    @classmethod
    def parse_relation_results(cls, relation_results: RelationResults) -> dict:
        materialized_view_config = relation_results.get("materialized_view")
        if isinstance(materialized_view_config, agate.Table):
            materialized_view = cls._get_first_row(materialized_view_config)
        else:
            raise DbtRuntimeError("Unsupported type returned ex. None")

        config_dict = {
            "materialized_view_name": materialized_view.get("materialized_view_name"),
            "schema_name": materialized_view.get("schema"),
            "database_name": materialized_view.get("database"),
            "hours_to_expiration": materialized_view.get("hours_to_expiration"),
            "kms_key_name": materialized_view.get("kms_key_name"),
            "labels": materialized_view.get("labels"),
        }

        if materialized_view.get("auto_refresh"):
            config_dict.update(
                {
                    "auto_refresh": BigQueryAutoRefreshConfig.parse_relation_results(
                        materialized_view
                    )
                }
            )

        if materialized_view.get("partition"):
            config_dict.update(
                {"partition": BigQueryPartitionConfig.parse_relation_results(materialized_view)}
            )

        if materialized_view.get("cluster"):
            config_dict.update(
                {"cluster": BigQueryClusterConfig.parse_relation_results(materialized_view)}
            )

        return config_dict


@dataclass
class BigQueryMaterializedViewConfigChangeset:
    partition: Optional[BigQueryPartitionConfigChange] = None
    cluster: Optional[BigQueryClusterConfigChange] = None
    auto_refresh: Optional[BigQueryAutoRefreshConfigChange] = None
    kms_key_name: Optional[str] = None
    labels: Optional[Dict[str, str]] = None

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
