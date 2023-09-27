from dataclasses import dataclass
from typing import Optional

import agate
from dbt.adapters.relation_configs import (
    RelationConfigChange,
    RelationConfigValidationMixin,
)
from dbt.adapters.bigquery.relation_configs.base import BigQueryReleationConfigBase
from dbt.contracts.graph.nodes import ModelNode
from dbt.adapters.bigquery.utility import bool_setting


@dataclass(frozen=True, eq=True, unsafe_hash=True)
class BigQueryAutoRefreshConfig(BigQueryReleationConfigBase, RelationConfigValidationMixin):
    """
    This config dictionary is comprised of three table options all centered around auto_refresh
    - enable_refresh: Enables autoamtic refresh of materialized view when base table is
      updated.
    - refresh_interval_minutes: frequency at which a materialized view will be refeshed.
        - Note: (default is 30 minutes)
    - max_staleness: if the last refresh is within max_staleness interval,
      BigQuery returns data directly from the materialized view without reading base table.
      Otherwise it reads from the base to return results withing the staleness interval.
    """

    enable_refresh: Optional[bool] = True
    refresh_interval_minutes: Optional[int] = 30
    max_staleness: Optional[str] = None

    @classmethod
    def from_dict(cls, config_dict) -> "BigQueryAutoRefreshConfig":
        kwargs_dict = {
            "enable_refresh": config_dict.get("enabled_refresh"),
            "refresh_interval_minutes": config_dict.get("refresh_interval_minutes"),
            "max_staleness": config_dict.get("max_staleness"),
        }
        auto_refresh: "BigQueryAutoRefreshConfig" = super().from_dict(kwargs_dict)  # type: ignore
        return auto_refresh

    @classmethod
    def parse_model_node(cls, model_node: ModelNode) -> dict:
        config_dict = {}
        raw_autorefresh_value = model_node.config.extra.get("enable_refresh")

        if raw_autorefresh_value:
            auto_refresh_value = bool_setting(raw_autorefresh_value)
            config_dict.update({"enable_refersh": auto_refresh_value})

        if refresh_interval_minutes := model_node.config.extra.get("refresh_interval_minutes"):
            config_dict.update({"refresh_interval_minutes": refresh_interval_minutes})

        if max_staleness := model_node.config.extra.get("max_staleness"):
            config_dict.update({"max_staleness": max_staleness})

        return config_dict

    @classmethod
    def parse_relation_results(cls, relation_results_entry: agate.Row) -> dict:
        config_dict = {}
        if enable_refresh := relation_results_entry.get("enable_refresh"):
            auto_refresh_value = bool_setting(enable_refresh)
            config_dict.update({"enable_refresh": auto_refresh_value})

        if refresh_interval_minutes := relation_results_entry.get("refresh_interval_minutes"):
            config_dict.update({"refresh_interval_minutes": refresh_interval_minutes})

        if max_staleness := relation_results_entry.get("max_staleness"):
            config_dict.update({"max_staleness": max_staleness})

        return config_dict


@dataclass(frozen=True, eq=True, unsafe_hash=True)
class BigQueryAutoRefreshConfigChange(RelationConfigChange):
    context: BigQueryAutoRefreshConfig

    @property
    def requires_full_refresh(self) -> bool:
        return False
