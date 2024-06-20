from dataclasses import dataclass, fields
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Optional, Set

from dbt.adapters.contracts.relation import (
    ComponentName,
    RelationConfig,
)
from dbt.adapters.relation_configs import (
    RelationConfigValidationMixin,
    RelationConfigValidationRule,
)
from dbt_common.exceptions import DbtRuntimeError
from google.cloud.bigquery import Table as BigQueryTable
from typing_extensions import Self

from dbt.adapters.bigquery.relation_configs._base import (
    BigQueryBaseRelationConfig,
    BigQueryRelationConfigChange,
)
from dbt.adapters.bigquery.relation_configs._cluster import BigQueryClusterConfig
from dbt.adapters.bigquery.relation_configs._partition import PartitionConfig
from dbt.adapters.bigquery.utility import bool_setting, float_setting


@dataclass(frozen=True, eq=True, unsafe_hash=True)
class BigQueryMaterializedViewConfig(BigQueryBaseRelationConfig, RelationConfigValidationMixin):
    """
    This config follow the specs found here:
    https://cloud.google.com/bigquery/docs/reference/standard-sql/data-definition-language#create_materialized_view_statement
    https://cloud.google.com/bigquery/docs/reference/standard-sql/data-definition-language#materialized_view_option_list

    The following parameters are configurable by dbt:
    - table_id: name of the materialized view
    - dataset_id: dataset name of the materialized view
    - project_id: project name of the database
    - options: options that get set in `SET OPTIONS()` clause
    - partition: object containing partition information
    - cluster: object containing cluster information

    The following options are configurable by dbt:
    - enable_refresh: turns on/off automatic refresh
    - refresh_interval_minutes: the refresh interval in minutes, when enable_refresh is True
    - expiration_timestamp: the expiration of data in the underlying table
    - max_staleness: the oldest data can be before triggering a refresh
    - allow_non_incremental_definition: allows non-incremental reloads, requires max_staleness
    - kms_key_name: the name of the keyfile
    - description: the comment to add to the materialized view
    - labels: labels to add to the materialized view

    Note:
    BigQuery allows options to be "unset" in the sense that they do not contain a value (think `None` or `null`).
    This can be counterintuitive when that option is a boolean; it introduces a third value, in particular
    a value that behaves "false-y". The practice is to mimic the data platform's inputs to the extent
    possible to minimize any translation confusion between dbt docs and the platform's (BQ's) docs.
    The values `False` and `None` will behave differently when producing the DDL options:
    - `False` will show up in the statement submitted to BQ with the value `False`
    - `None` will not show up in the statement submitted to BQ at all
    """

    table_id: str
    dataset_id: str
    project_id: str
    partition: Optional[PartitionConfig] = None
    cluster: Optional[BigQueryClusterConfig] = None
    enable_refresh: Optional[bool] = True
    refresh_interval_minutes: Optional[float] = 30
    expiration_timestamp: Optional[datetime] = None
    max_staleness: Optional[str] = None
    allow_non_incremental_definition: Optional[bool] = None
    kms_key_name: Optional[str] = None
    description: Optional[str] = None
    labels: Optional[Dict[str, str]] = None

    @property
    def validation_rules(self) -> Set[RelationConfigValidationRule]:
        # validation_check is what is allowed
        return {
            RelationConfigValidationRule(
                validation_check=self.allow_non_incremental_definition is not True
                or self.max_staleness is not None,
                validation_error=DbtRuntimeError(
                    "Please provide a setting for max_staleness when enabling allow_non_incremental_definition.\n"
                    "Received:\n"
                    f"    allow_non_incremental_definition: {self.allow_non_incremental_definition}\n"
                    f"    max_staleness: {self.max_staleness}\n"
                ),
            ),
            RelationConfigValidationRule(
                validation_check=self.enable_refresh is True
                or all(
                    [self.max_staleness is None, self.allow_non_incremental_definition is None]
                ),
                validation_error=DbtRuntimeError(
                    "Do not provide a setting for refresh_interval_minutes, max_staleness, nor allow_non_incremental_definition when disabling enable_refresh.\n"
                    "Received:\n"
                    f"    enable_refresh: {self.enable_refresh}\n"
                    f"    max_staleness: {self.max_staleness}\n"
                    f"    allow_non_incremental_definition: {self.allow_non_incremental_definition}\n"
                ),
            ),
        }

    @classmethod
    def from_dict(cls, config_dict: Dict[str, Any]) -> Self:
        # required
        kwargs_dict: Dict[str, Any] = {
            "table_id": cls._render_part(ComponentName.Identifier, config_dict["table_id"]),
            "dataset_id": cls._render_part(ComponentName.Schema, config_dict["dataset_id"]),
            "project_id": cls._render_part(ComponentName.Database, config_dict["project_id"]),
        }

        # optional
        optional_settings = {
            "partition": PartitionConfig.parse,
            "cluster": BigQueryClusterConfig.from_dict,
            "enable_refresh": bool_setting,
            "refresh_interval_minutes": float_setting,
            "expiration_timestamp": None,
            "max_staleness": None,
            "allow_non_incremental_definition": bool_setting,
            "kms_key_name": None,
            "description": None,
            "labels": None,
        }

        for setting, parser in optional_settings.items():
            value = config_dict.get(setting)
            if value is not None and parser is not None:
                kwargs_dict.update({setting: parser(value)})  # type: ignore
            elif value is not None:
                kwargs_dict.update({setting: value})

        materialized_view: Self = super().from_dict(kwargs_dict)  # type: ignore
        return materialized_view

    @classmethod
    def parse_relation_config(cls, relation_config: RelationConfig) -> Dict[str, Any]:
        config_extras = relation_config.config.extra  # type: ignore

        config_dict: Dict[str, Any] = {
            # required
            "table_id": relation_config.identifier,
            "dataset_id": relation_config.schema,
            "project_id": relation_config.database,
            # optional - no transformations
            "enable_refresh": config_extras.get("enable_refresh"),
            "refresh_interval_minutes": config_extras.get("refresh_interval_minutes"),
            "max_staleness": config_extras.get("max_staleness"),
            "allow_non_incremental_definition": config_extras.get(
                "allow_non_incremental_definition"
            ),
            "kms_key_name": config_extras.get("kms_key_name"),
            "description": config_extras.get("description"),
            "labels": config_extras.get("labels"),
        }

        # optional - transformations
        if relation_config.config.get("partition_by"):  # type: ignore
            config_dict["partition"] = PartitionConfig.parse_model_node(relation_config)

        if relation_config.config.get("cluster_by"):  # type: ignore
            config_dict["cluster"] = BigQueryClusterConfig.parse_relation_config(relation_config)

        if hours_to_expiration := config_extras.get("hours_to_expiration"):
            config_dict["expiration_timestamp"] = datetime.now(tz=timezone.utc) + timedelta(
                hours=hours_to_expiration
            )

        if relation_config.config.persist_docs and config_extras.get("description"):  # type: ignore
            config_dict["description"] = config_extras.get("description")

        return config_dict

    @classmethod
    def parse_bq_table(cls, table: BigQueryTable) -> Dict[str, Any]:
        config_dict: Dict[str, Any] = {
            # required
            "table_id": table.table_id,
            "dataset_id": table.dataset_id,
            "project_id": table.project,
            # optional - no transformation
            "enable_refresh": table.mview_enable_refresh,
            "expiration_timestamp": table.expires,
            "allow_non_incremental_definition": table._properties.get("materializedView", {}).get(
                "allowNonIncrementalDefinition"
            ),
            "kms_key_name": getattr(
                getattr(table, "encryption_configuration"), "kms_key_name", None
            ),
            "description": table.description,
            "labels": table.labels if table.labels != {} else None,
        }

        # optional
        if table.time_partitioning or table.range_partitioning:
            config_dict.update({"partition": PartitionConfig.parse_bq_table(table)})

        if table.clustering_fields:
            config_dict.update({"cluster": BigQueryClusterConfig.parse_bq_table(table)})

        if refresh_interval_seconds := table.mview_refresh_interval.seconds:
            config_dict.update({"refresh_interval_minutes": refresh_interval_seconds / 60})

        if max_staleness := table._properties.get("maxStaleness"):
            config_dict.update({"max_staleness": f"INTERVAL '{max_staleness}' YEAR TO SECOND"})

        return config_dict


@dataclass
class BigQueryMaterializedViewConfigChangeset:
    """
    A collection of changes on a materialized view.

    Note: We don't watch for `expiration_timestamp` because it only gets set on the initial creation.
    It would naturally change every time since it's set via `hours_to_expiration`, which would push out
    the calculated `expiration_timestamp`.
    """

    partition: Optional[BigQueryRelationConfigChange] = None
    cluster: Optional[BigQueryRelationConfigChange] = None
    enable_refresh: Optional[BigQueryRelationConfigChange] = None
    refresh_interval_minutes: Optional[BigQueryRelationConfigChange] = None
    max_staleness: Optional[BigQueryRelationConfigChange] = None
    allow_non_incremental_definition: Optional[BigQueryRelationConfigChange] = None
    kms_key_name: Optional[BigQueryRelationConfigChange] = None
    description: Optional[BigQueryRelationConfigChange] = None
    labels: Optional[BigQueryRelationConfigChange] = None

    @property
    def requires_full_refresh(self) -> bool:
        return any(
            [
                getattr(self, field.name).requires_full_refresh
                if getattr(self, field.name)
                else False
                for field in fields(self)
            ]
        )

    @property
    def has_changes(self) -> bool:
        return any([getattr(self, field.name) is not None for field in fields(self)])
