from dataclasses import dataclass
from typing import Any, Dict, Optional

import agate
from dbt.adapters.relation_configs import RelationResults
from dbt.contracts.graph.nodes import ModelNode
from dbt.contracts.relation import ComponentName

from dbt.adapters.bigquery.relation_configs._base import BigQueryRelationConfigBase
from dbt.adapters.bigquery.relation_configs.options import (
    BigQueryOptionsConfig,
    BigQueryOptionsConfigChange,
)
from dbt.adapters.bigquery.relation_configs.partition import PartitionConfig
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
    - schema_name: dataset name of the materialized view
    - database_name: project name of the database
    - options: options that get set in `SET OPTIONS()` clause
    - partition: object containing partition information
    - cluster: object containing cluster information
    """

    materialized_view_name: str
    schema_name: str
    database_name: str
    options: BigQueryOptionsConfig
    partition: Optional[PartitionConfig] = None
    cluster: Optional[BigQueryClusterConfig] = None

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
            "options": BigQueryOptionsConfig.from_dict(config_dict["options"]),
        }

        # optional
        if partition := config_dict.get("partition"):
            kwargs_dict.update({"partition": PartitionConfig.parse(partition)})

        if cluster := config_dict.get("cluster"):
            kwargs_dict.update({"cluster": BigQueryClusterConfig.from_dict(cluster)})

        materialized_view: "BigQueryMaterializedViewConfig" = super().from_dict(kwargs_dict)  # type: ignore
        return materialized_view

    @classmethod
    def parse_model_node(cls, model_node: ModelNode) -> Dict[str, Any]:
        config_dict = {
            "materialized_view_name": model_node.identifier,
            "schema_name": model_node.schema,
            "database_name": model_node.database,
            # despite this being a foreign object, there will always be options because of defaults
            "options": BigQueryOptionsConfig.parse_model_node(model_node),
        }

        # optional
        if "partition_by" in model_node.config:
            config_dict.update({"partition": PartitionConfig.parse_model_node(model_node)})

        if "cluster_by" in model_node.config:
            config_dict.update({"cluster": BigQueryClusterConfig.parse_model_node(model_node)})

        return config_dict

    @classmethod
    def parse_relation_results(cls, relation_results: RelationResults) -> Dict[str, Any]:
        materialized_view_config: agate.Table = relation_results.get("materialized_view")  # type: ignore
        materialized_view: agate.Row = cls._get_first_row(materialized_view_config)

        config_dict = {
            "materialized_view_name": materialized_view.get("table_name"),
            "schema_name": materialized_view.get("table_schema"),
            "database_name": materialized_view.get("table_catalog"),
            # despite this being a foreign object, there will always be options because of defaults
            "options": BigQueryOptionsConfig.parse_relation_results(relation_results),
        }

        # optional
        if partition_by := relation_results.get("partition_by"):
            config_dict.update({"partition": PartitionConfig.parse_relation_results(partition_by)})  # type: ignore

        cluster_by: agate.Table = relation_results.get("cluster_by")  # type: ignore
        if len(cluster_by) > 0:
            config_dict.update(
                {"cluster": BigQueryClusterConfig.parse_relation_results(cluster_by)}
            )

        return config_dict


@dataclass
class BigQueryMaterializedViewConfigChangeset:
    options: Optional[BigQueryOptionsConfigChange] = None
    cluster: Optional[BigQueryClusterConfigChange] = None

    @property
    def requires_full_refresh(self) -> bool:
        return any(
            {
                self.options.requires_full_refresh if self.options else False,
                self.cluster.requires_full_refresh if self.cluster else False,
            }
        )

    @property
    def has_changes(self) -> bool:
        return any(
            {
                self.options if self.options else False,
                self.cluster if self.cluster else False,
            }
        )
