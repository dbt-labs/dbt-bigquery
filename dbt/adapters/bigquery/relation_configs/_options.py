from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Dict, Optional, Set

from dbt.adapters.relation_configs import (
    RelationConfigChange,
    RelationConfigChangeAction,
    RelationConfigValidationMixin,
    RelationConfigValidationRule,
)
from dbt.adapters.contracts.relation import RelationConfig
from dbt_common.exceptions import DbtRuntimeError
from google.cloud.bigquery import Table as BigQueryTable
from typing_extensions import Self

from dbt.adapters.bigquery.relation_configs._base import BigQueryBaseRelationConfig
from dbt.adapters.bigquery.utility import bool_setting, float_setting, sql_escape


@dataclass(frozen=True, eq=True, unsafe_hash=True)
class BigQueryOptionsConfig(BigQueryBaseRelationConfig, RelationConfigValidationMixin):
    """
    This config manages materialized view options. See the following for more information:
    - https://cloud.google.com/bigquery/docs/reference/standard-sql/data-definition-language#materialized_view_option_list

    Note:
        BigQuery allows options to be "unset" in the sense that they do not contain a value (think `None` or `null`).
        This can be counterintuitive when that option is a boolean; it introduces a third value, in particular
        a value that behaves "false-y". The practice is to mimic the data platform's inputs to the extent
        possible to minimize any translation confusion between dbt docs and the platform's (BQ's) docs.
        The values `False` and `None` will behave differently when producing the DDL options:
        - `False` will show up in the statement submitted to BQ with the value `False`
        - `None` will not show up in the statement submitted to BQ at all
    """

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

    def as_ddl_dict(self, include_nulls: Optional[bool] = False) -> Dict[str, Any]:
        """
        Return a representation of this object so that it can be passed into the `bigquery_options()` macro.

        Options should be flattened and filtered prior to passing into this method. For example:
        - the "auto refresh" set of options should be flattened into the root instead of stuck under "auto_refresh"
        - any option that comes in set as `None` will be unset; this happens mostly due to config changes
        """

        def boolean(x):
            return x

        def numeric(x):
            return x

        def string(x):
            return f"'{x}'"

        def escaped_string(x):
            return f'"""{sql_escape(x)}"""'

        def interval(x):
            return x

        def array(x):
            return list(x.items())

        option_formatters = {
            "enable_refresh": boolean,
            "refresh_interval_minutes": numeric,
            "expiration_timestamp": interval,
            "max_staleness": interval,
            "allow_non_incremental_definition": boolean,
            "kms_key_name": string,
            "description": escaped_string,
            "labels": array,
        }

        def formatted_option(name: str) -> Optional[Any]:
            value = getattr(self, name)
            if value is None and include_nulls:
                # used when altering relations to catch scenarios where non-defaulted options are "unset"
                return "NULL"
            elif value is None:
                return None
            formatter = option_formatters[name]
            return formatter(value)

        options = {
            option: formatted_option(option)
            for option in option_formatters
            if formatted_option(option) is not None
        }

        return options

    @classmethod
    def from_dict(cls, config_dict: Dict[str, Any]) -> Self:
        setting_formatters = {
            "enable_refresh": bool_setting,
            "refresh_interval_minutes": float_setting,
            "expiration_timestamp": None,
            "max_staleness": None,
            "allow_non_incremental_definition": bool_setting,
            "kms_key_name": None,
            "description": None,
            "labels": None,
        }

        def formatted_setting(name: str) -> Any:
            value = config_dict.get(name)
            if formatter := setting_formatters[name]:
                return formatter(value)
            return value

        kwargs_dict = {attribute: formatted_setting(attribute) for attribute in setting_formatters}

        # avoid picking up defaults on dependent options
        # e.g. don't set `refresh_interval_minutes` = 30 when the user has `enable_refresh` = False
        if kwargs_dict["enable_refresh"] is False:
            kwargs_dict.update(
                {
                    "refresh_interval_minutes": None,
                    "max_staleness": None,
                    "allow_non_incremental_definition": None,
                }
            )

        options: Self = super().from_dict(kwargs_dict)  # type: ignore
        return options

    @classmethod
    def parse_relation_config(cls, relation_config: RelationConfig) -> Dict[str, Any]:
        config_dict = {
            option: relation_config.config.extra.get(option)  # type: ignore
            for option in [
                "enable_refresh",
                "refresh_interval_minutes",
                "expiration_timestamp",
                "max_staleness",
                "allow_non_incremental_definition",
                "kms_key_name",
                "description",
                "labels",
            ]
        }

        # update dbt-specific versions of these settings
        if hours_to_expiration := relation_config.config.extra.get(  # type: ignore
            "hours_to_expiration"
        ):  # type: ignore
            config_dict.update(
                {"expiration_timestamp": datetime.now() + timedelta(hours=hours_to_expiration)}
            )
        if not relation_config.config.persist_docs:  # type: ignore
            del config_dict["description"]

        return config_dict

    @classmethod
    def parse_bq_table(cls, table: BigQueryTable) -> Dict[str, Any]:
        config_dict = {
            "enable_refresh": table.mview_enable_refresh,
            "refresh_interval_minutes": table.mview_refresh_interval.seconds / 60,
            "expiration_timestamp": table.expires,
            "max_staleness": (
                f"INTERVAL '{table._properties.get('maxStaleness')}' YEAR TO SECOND"
                if table._properties.get("maxStaleness")
                else None
            ),
            "allow_non_incremental_definition": table._properties.get("materializedView", {}).get(
                "allowNonIncrementalDefinition"
            ),
            "description": table.description,
        }

        # map the empty dict to None
        if labels := table.labels:
            config_dict.update({"labels": labels})

        if encryption_configuration := table.encryption_configuration:
            config_dict.update({"kms_key_name": encryption_configuration.kms_key_name})
        return config_dict


@dataclass(frozen=True, eq=True, unsafe_hash=True)
class BigQueryOptionsConfigChange(RelationConfigChange):
    context: BigQueryOptionsConfig

    @property
    def requires_full_refresh(self) -> bool:
        return self.action != RelationConfigChangeAction.alter
