from dataclasses import dataclass
from typing import Any, Dict, Optional

import agate
from dbt.adapters.relation_configs import RelationConfigChange
from dbt.contracts.graph.nodes import ModelNode

from dbt.adapters.bigquery.relation_configs._base import BigQueryRelationConfigBase
from dbt.adapters.bigquery.utility import bool_setting


@dataclass(frozen=True, eq=True, unsafe_hash=True)
class BigQueryAutoRefreshConfig(BigQueryRelationConfigBase):
    """
    This config manages materialized view options supporting automatic refresh. See the following for more information:
    - https://cloud.google.com/bigquery/docs/reference/standard-sql/data-definition-language#materialized_view_option_list
    - https://cloud.google.com/bigquery/docs/materialized-views-create#manage_staleness_and_refresh_frequency

    - enable_refresh: enables automatic refresh based on `refresh_interval_minutes`
    - refresh_interval_minutes: frequency at which a materialized view will be refreshed
    - max_staleness: if the last refresh is within the max_staleness interval,
       BigQuery returns data directly from the materialized view (faster/cheaper) without reading the base table,
       otherwise it reads from the base table (slower/more expensive) to meet the staleness requirement
    """

    enable_refresh: Optional[bool] = True
    refresh_interval_minutes: Optional[int] = 30
    max_staleness: Optional[str] = None

    @classmethod
    def from_dict(cls, config_dict: Dict[str, Any]) -> "BigQueryAutoRefreshConfig":
        kwargs_dict = {}

        # optional
        if "enable_refresh" in config_dict:  # boolean
            kwargs_dict.update({"enable_refresh": config_dict.get("enable_refresh")})
        if refresh_interval_minutes := config_dict.get("refresh_interval_minutes"):
            kwargs_dict.update({"refresh_interval_minutes": refresh_interval_minutes})
        if max_staleness := config_dict.get("max_staleness"):
            kwargs_dict.update({"max_staleness": max_staleness})

        auto_refresh: "BigQueryAutoRefreshConfig" = super().from_dict(kwargs_dict)  # type: ignore
        return auto_refresh

    @classmethod
    def parse_model_node(cls, model_node: ModelNode) -> Dict[str, Any]:
        config_dict = {}

        # check for the key since this is a boolean
        if "enable_refresh" in model_node.config.extra:
            enable_refresh = model_node.config.extra.get("enable_refresh")
            config_dict.update({"enable_refresh": bool_setting(enable_refresh)})

        if refresh_interval_minutes := model_node.config.extra.get("refresh_interval_minutes"):
            config_dict.update({"refresh_interval_minutes": refresh_interval_minutes})

        if max_staleness := model_node.config.extra.get("max_staleness"):
            config_dict.update({"max_staleness": max_staleness})

        return config_dict

    @classmethod
    def parse_relation_results(cls, relation_results_entry: agate.Row) -> Dict[str, Any]:  # type: ignore
        config_dict = {
            "enable_refresh": bool_setting(relation_results_entry.get("enable_refresh")),
            "refresh_interval_minutes": relation_results_entry.get("refresh_interval_minutes"),
            "max_staleness": relation_results_entry.get("max_staleness"),
        }
        return config_dict


@dataclass(frozen=True, eq=True, unsafe_hash=True)
class BigQueryAutoRefreshConfigChange(RelationConfigChange):
    context: BigQueryAutoRefreshConfig

    @property
    def requires_full_refresh(self) -> bool:
        return False
