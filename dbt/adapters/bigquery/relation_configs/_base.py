from dataclasses import dataclass
from typing import Optional

import agate
from dbt.adapters.base.relation import Policy
from dbt.adapters.relation_configs import RelationConfigBase
from google.cloud.bigquery import Table as BigQueryTable

from dbt.adapters.bigquery.relation_configs._policies import (
    BigQueryIncludePolicy,
    BigQueryQuotePolicy,
)
from dbt.contracts.graph.nodes import ModelNode
from dbt.contracts.relation import ComponentName


@dataclass(frozen=True, eq=True, unsafe_hash=True)
class BigQueryBaseRelationConfig(RelationConfigBase):
    @classmethod
    def include_policy(cls) -> Policy:
        return BigQueryIncludePolicy()

    @classmethod
    def quote_policy(cls) -> Policy:
        return BigQueryQuotePolicy()

    @classmethod
    def from_model_node(cls, model_node: ModelNode) -> "BigQueryBaseRelationConfig":
        relation_config = cls.parse_model_node(model_node)
        relation = cls.from_dict(relation_config)
        return relation  # type: ignore

    @classmethod
    def parse_model_node(cls, model_node: ModelNode) -> dict:
        raise NotImplementedError(
            "`parse_model_node()` needs to be implemented on this RelationConfigBase instance"
        )

    @classmethod
    def from_bq_table(cls, table: BigQueryTable) -> "BigQueryBaseRelationConfig":
        relation_config = cls.parse_bq_table(table)
        relation = cls.from_dict(relation_config)
        return relation  # type: ignore

    @classmethod
    def parse_bq_table(cls, table: BigQueryTable) -> dict:
        raise NotImplementedError("`parse_bq_table()` is not implemented for this relation type")

    @classmethod
    def _render_part(cls, component: ComponentName, value: Optional[str]) -> Optional[str]:
        if cls.include_policy().get_part(component) and value:
            if cls.quote_policy().get_part(component):
                return f'"{value}"'
            return value.lower()
        return None

    @classmethod
    def _get_first_row(cls, results: agate.Table) -> agate.Row:
        try:
            return results.rows[0]
        except IndexError:
            return agate.Row(values=set())
