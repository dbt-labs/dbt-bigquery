from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Union
from dbt.adapters.relation_configs.config_base import RelationConfigBase
from dbt.adapters.relation_configs.config_validation import RelationConfigValidationMixin
from dbt.contracts.graph.nodes import ModelNode


@dataclass(frozen=True, eq=True, unsafe_hash=True)
class BigQueryMaterializedViewConfig(RelationConfigBase, RelationConfigValidationMixin):
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
    - allow_non_incremental_definition:
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
    cluster_by: Optional[Union[List[str], str]] = None
    partition_by: Optional[Dict[str, Any]] = None
    enable_refresh: bool = True
    refresh_interval_minutes: float = 30
    hours_to_expiration: Optional[int] = None
    max_staleness: Optional[int] = None
    allow_non_incremental_definition: Optional[bool] = None
    kms_key_name: Optional[str] = None
    friendly_name: Optional[str] = None
    description: Optional[str] = None
    labels: Optional[Dict[str, str]] = None

    @classmethod
    def parse_model_node(cls, model_node: ModelNode) -> dict:
        config_dict = {
            "materialized_view_name": model_node.identifier,
            "schema_name": model_node.schema,
            "database_name": model_node.database,
        }

        return config_dict
