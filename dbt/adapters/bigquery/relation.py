from dataclasses import dataclass
from typing import FrozenSet, Optional, TypeVar

from itertools import chain, islice
from dbt.context.providers import RuntimeConfigObject
from dbt.adapters.base.relation import BaseRelation, ComponentName, InformationSchema
from dbt.adapters.relation_configs import RelationConfigChangeAction
from dbt.adapters.bigquery.relation_configs import (
    BigQueryClusterConfigChange,
    BigQueryMaterializedViewConfig,
    BigQueryMaterializedViewConfigChangeset,
    BigQueryOptionsConfigChange,
    BigQueryPartitionConfigChange,
)
from dbt.contracts.graph.nodes import ModelNode
from dbt.contracts.relation import RelationType
from dbt.exceptions import CompilationError
from dbt.utils import filter_null_values


Self = TypeVar("Self", bound="BigQueryRelation")


@dataclass(frozen=True, eq=False, repr=False)
class BigQueryRelation(BaseRelation):
    quote_character: str = "`"
    location: Optional[str] = None
    renameable_relations: FrozenSet[RelationType] = frozenset({RelationType.Table})
    replaceable_relations: FrozenSet[RelationType] = frozenset(
        {RelationType.Table, RelationType.View}
    )

    def matches(
        self,
        database: Optional[str] = None,
        schema: Optional[str] = None,
        identifier: Optional[str] = None,
    ) -> bool:
        search = filter_null_values(
            {
                ComponentName.Database: database,
                ComponentName.Schema: schema,
                ComponentName.Identifier: identifier,
            }
        )

        if not search:
            # nothing was passed in
            pass

        for k, v in search.items():
            if not self._is_exactish_match(k, v):
                return False

        return True

    @property
    def project(self):
        return self.database

    @property
    def dataset(self):
        return self.schema

    @classmethod
    def materialized_view_from_model_node(
        cls, model_node: ModelNode
    ) -> BigQueryMaterializedViewConfig:
        return BigQueryMaterializedViewConfig.from_model_node(model_node)  # type: ignore

    @classmethod
    def materialized_view_config_changeset(
        cls,
        existing_materialized_view: BigQueryMaterializedViewConfig,
        runtime_config: RuntimeConfigObject,
    ) -> Optional[BigQueryMaterializedViewConfigChangeset]:
        config_change_collection = BigQueryMaterializedViewConfigChangeset()
        new_materialized_view = cls.materialized_view_from_model_node(runtime_config.model)

        if new_materialized_view.options != existing_materialized_view.options:
            config_change_collection.options = BigQueryOptionsConfigChange(
                action=RelationConfigChangeAction.alter,
                context=new_materialized_view.options,
            )

        if new_materialized_view.partition != existing_materialized_view.partition:
            # the existing PartitionConfig is not hashable, but since we need to do
            # a full refresh either way, we don't need to provide a context
            config_change_collection.partition = BigQueryPartitionConfigChange(
                action=RelationConfigChangeAction.alter,
            )

        if new_materialized_view.cluster != existing_materialized_view.cluster:
            config_change_collection.cluster = BigQueryClusterConfigChange(
                action=RelationConfigChangeAction.alter,
                context=new_materialized_view.cluster,
            )

        if config_change_collection.has_changes:
            return config_change_collection
        return None

    def information_schema(self, identifier: Optional[str] = None) -> "BigQueryInformationSchema":
        return BigQueryInformationSchema.from_relation(self, identifier)


@dataclass(frozen=True, eq=False, repr=False)
class BigQueryInformationSchema(InformationSchema):
    quote_character: str = "`"
    location: Optional[str] = None

    @classmethod
    def get_include_policy(cls, relation, information_schema_view):
        schema = True
        if information_schema_view in ("SCHEMATA", "SCHEMATA_OPTIONS", None):
            schema = False

        identifier = True
        if information_schema_view == "__TABLES__":
            identifier = False

        # In the future, let's refactor so that location/region can also be a
        # ComponentName, so that we can have logic like:
        #
        # region = False
        # if information_schema_view == "OBJECT_PRIVILEGES":
        #     region = True

        return relation.include_policy.replace(
            schema=schema,
            identifier=identifier,
        )

    def get_region_identifier(self) -> str:
        region_id = f"region-{self.location}"
        return self.quoted(region_id)

    @classmethod
    def from_relation(cls, relation, information_schema_view):
        info_schema = super().from_relation(relation, information_schema_view)
        if information_schema_view == "OBJECT_PRIVILEGES":
            # OBJECT_PRIVILEGES require a location.  If the location is blank there is nothing
            # the user can do about it.
            if not relation.location:
                msg = (
                    f'No location/region found when trying to retrieve "{information_schema_view}"'
                )
                raise CompilationError(msg)
            info_schema = info_schema.incorporate(location=relation.location)
        return info_schema

    # override this method to interpolate the region identifier,
    # if a location is required for this information schema view
    def _render_iterator(self):
        iterator = super()._render_iterator()
        if self.location:
            return chain(
                islice(iterator, 1),  # project,
                [(None, self.get_region_identifier())],  # region id,
                islice(iterator, 1, None),  # remaining components
            )
        else:
            return iterator

    def replace(self, **kwargs):
        if "information_schema_view" in kwargs:
            view = kwargs["information_schema_view"]
            # we also need to update the include policy, unless the caller did
            # in which case it's their problem
            if "include_policy" not in kwargs:
                kwargs["include_policy"] = self.get_include_policy(self, view)
        return super().replace(**kwargs)
