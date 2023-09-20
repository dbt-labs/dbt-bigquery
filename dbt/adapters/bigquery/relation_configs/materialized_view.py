from dataclasses import dataclass
from typing import Any, Dict, List, FrozenSet, Optional, Union

import agate
from dbt.exceptions import DbtRuntimeError
from dbt.adapters.relation_configs.config_change import RelationConfigChange
from dbt.adapters.relation_configs.config_base import RelationResults
from dbt.adapters.relation_configs.config_validation import RelationConfigValidationMixin
from dbt.contracts.graph.nodes import ModelNode
from dbt.contracts.relation import ComponentName
from dbt.adapters.bigquery.relation_configs.base import BigQueryReleationConfigBase


@dataclass(frozen=True, eq=True, unsafe_hash=True)
class BigQueryMaterializedViewConfig(BigQueryReleationConfigBase, RelationConfigValidationMixin):
    """
    This config follow the specs found here:
    https://cloud.google.com/bigquery/docs/reference/standard-sql/data-definition-language#create_materialized_view_statement

    The following parameters are configurable by dbt:
    - materialized_view_name: Name of the materialized view
    - schema: Dataset name of the materialized view
    - database: Project name of the database
    - cluster_by: A comma-seperated list of of col references to determine cluster.
        - Note: Can contain up to four colms in list.
    - partition_by: Expression to describe how to partition materialized view.
        - Note: Must be partitioned in the same was as base table is partitioned.
    - enable_refresh: Enables autoamtic refresh of materialized view when base table is
      updated.
    - refresh_interval_minutes: frequency at which a materialized view will be refeshed.
        - Note: (default is 30 minutes)
    - hours_to_expiration: The time when table expires.
        - Note: If not set table persists
    - max_staleness: if the last refresh is within max_staleness interval,
      BigQuery returns data directly from the materialized view without reading base table.
      Otherwise it reads from the base to return results withing the staleness interval.
    - kms_key_name: user defined Cloud KMS encryption key.
    - friendly_name: A descriptive name for this table.
    - description: A user-friendly description of this table.
    - labels: used to organized and group table
        - Note on usage can be found

    There are currently no non-configurable parameters.
    """

    materialized_view_name: str
    schema_name: str
    database_name: str
    cluster_by: Optional[Union[FrozenSet[List[str]], str]] = None
    partition_by: Optional[FrozenSet[Dict[str, Any]]] = None
    partition_expiration_date: Optional[int] = None
    enable_refresh: Optional[bool] = True
    refresh_interval_minutes: Optional[int] = 30
    hours_to_expiration: Optional[int] = None
    max_staleness: Optional[str] = None
    kms_key_name: Optional[str] = None
    friendly_name: Optional[str] = None
    description: Optional[str] = None
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
            "cluster_by": config_dict.get("cluster_by"),
            "partition_by": config_dict.get("partition_by"),
            "partition_expiration_date": config_dict.get("partition_expiration_date"),
            "enable_refresh": config_dict.get("enable_refresh"),
            "refresh_interval_minutes": config_dict.get("refresh_interval_minutes"),
            "hours_to_expiration": config_dict.get("hours_to_expiration"),
            "max_staleness": config_dict.get("max_staleness"),
            "kms_key_name": config_dict.get("kms_key_name"),
            "friendly_name": config_dict.get("friendly_name"),
            "description": config_dict.get("description"),
            "labels": config_dict.get("labels"),
        }

        materialized_view: "BigQueryMaterializedViewConfig" = super().from_dict(kwargs_dict)  # type: ignore
        return materialized_view

    @classmethod
    def parse_model_node(cls, model_node: ModelNode) -> dict:
        config_dict = {
            "materialized_view_name": model_node.identifier,
            "schema_name": model_node.schema,
            "database_name": model_node.database,
            "cluster_by": model_node.config.extra.get("cluster_by"),
            "partition_by": model_node.config.extra.get("partition_by"),
            "partition_expiration_date": model_node.config.extra.get("partition_expiration_date"),
            "refresh_interval_minutes": model_node.config.extra.get("refresh_interval_minutes"),
            "hours_to_expiration": model_node.config.extra.get("hours_to_expiration"),
            "max_staleness": model_node.config.extra.get("max_staleness"),
            "kms_key_name": model_node.config.extra.get("kms_key_name"),
            "friendly_name": model_node.config.extra.get("friendly_name"),
            "description": model_node.config.extra.get("description"),
            "labels": model_node.config.extra.get("labels"),
        }

        autorefresh_value = model_node.config.extra.get("enable_refresh")
        if autorefresh_value is not None:
            if isinstance(autorefresh_value, bool):
                config_dict["enable_refresh"] = autorefresh_value
            elif isinstance(autorefresh_value, str):
                lower_autorefresh_value = autorefresh_value.lower()
                if lower_autorefresh_value == "true":
                    config_dict["enable_refresh"] = True
                elif lower_autorefresh_value == "false":
                    config_dict["enable_refresh"] = False
                else:
                    raise ValueError(
                        "Invalide enable_refresh representation. Please used excepted value ex.(True, 'true', 'True')"
                    )
            else:
                raise TypeError("Invalid autorefresh value: expecting boolean or str.")

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
            "cluster_by": materialized_view.get("cluster_by"),
            "partition_by": materialized_view.get("partition_by"),
            "partition_expiration_date": materialized_view.get("partition_expiration_date"),
            "enable_refresh": materialized_view.get("enabled_refresh"),
            "refresh_interval_minutes": materialized_view.get("refresh_interval_minutes"),
            "hours_to_expiration": materialized_view.get("hours_to_expiration"),
            "max_staleness": materialized_view.get("max_staleness"),
            "kms_key_name": materialized_view.get("kms_key_name"),
            "friendly_name": materialized_view.get("friendly_name"),
            "description": materialized_view.get("description"),
            "labels": materialized_view.get("labels"),
        }

        return config_dict


@dataclass(frozen=True, eq=True, unsafe_hash=True)
class BigQueryAutoRefreshConfigChange(RelationConfigChange):
    context: Optional[bool] = None

    @property
    def requires_full_refresh(self) -> bool:
        return False


@dataclass(frozen=True, eq=True, unsafe_hash=True)
class BigQueryPartitionConfigChange(RelationConfigChange):
    context: Optional[FrozenSet[Dict[str, Any]]] = None

    @property
    def requires_full_refresh(self) -> bool:
        return True


@dataclass(frozen=True, eq=True, unsafe_hash=True)
class BigQueryClusterConfigChange(RelationConfigChange):
    context: Optional[Union[FrozenSet[List[str]], str]] = None

    @property
    def requires_full_refresh(self) -> bool:
        return True


@dataclass
class BigQueryMaterializedViewConfigChangeset:
    partition_by: Optional[BigQueryPartitionConfigChange] = None
    partition_expiration_days: Optional[BigQueryPartitionConfigChange] = None
    cluster_by: Optional[BigQueryClusterConfigChange] = None
    auto_refresh: Optional[BigQueryAutoRefreshConfigChange] = None

    @property
    def requires_full_refresh(self) -> bool:
        return any(
            {
                self.auto_refresh.requires_full_refresh if self.auto_refresh else False,
                self.partition_by.requires_full_refresh if self.partition_by else False,
                self.cluster_by.requires_full_refresh if self.cluster_by else False,
            }
        )

    @property
    def has_changes(self) -> bool:
        return any(
            {
                self.partition_by if self.partition_by else False,
                self.cluster_by if self.cluster_by else False,
                self.auto_refresh if self.auto_refresh else False,
            }
        )
