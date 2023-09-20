from dataclasses import dataclass, field
from typing import Optional

from itertools import chain, islice
from dbt.context.providers import RuntimeConfigObject
from dbt.adapters.base.relation import BaseRelation, ComponentName, InformationSchema
from dbt.adapters.relation_configs import RelationResults, RelationConfigChangeAction
from dbt.adapters.bigquery.relation_configs import (
    BigQueryIncludePolicy,
    BigQueryQuotePolicy,
    BigQueryMaterializedViewConfig,
    BigQueryMaterializedViewConfigChangeset,
    BigQueryAutoRefreshConfigChange,
    BigQueryClusterConfigChange,
    BigQueryPartitionConfigChange,
)
from dbt.contracts.relation import RelationType
from dbt.exceptions import CompilationError
from dbt.utils import filter_null_values
from typing import TypeVar


Self = TypeVar("Self", bound="BigQueryRelation")


@dataclass(frozen=True, eq=False, repr=False)
class BigQueryRelation(BaseRelation):
    quote_character: str = "`"
    location: Optional[str] = None
    include_policy: BigQueryIncludePolicy = field(default_factory=lambda: BigQueryIncludePolicy())
    quote_policy: BigQueryQuotePolicy = field(default_factory=lambda: BigQueryQuotePolicy())
    # why do we need to use default_factory here but we can assign it directly in dbt-postgres?
    renameable_relations = frozenset({RelationType.Table})
    replaceable_relations = frozenset({RelationType.Table, RelationType.View})

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
    def materialized_view_config_changeset(
        cls, relaation_results: RelationResults, runtime_config: RuntimeConfigObject
    ) -> Optional[BigQueryMaterializedViewConfigChangeset]:
        config_change_collection = BigQueryMaterializedViewConfigChangeset()
        existing_materialized_view = BigQueryMaterializedViewConfig.from_relation_results(
            relaation_results
        )
        new_materialized_view = BigQueryMaterializedViewConfig.from_model_node(
            runtime_config.model
        )
        assert isinstance(existing_materialized_view, BigQueryMaterializedViewConfig)
        assert isinstance(new_materialized_view, BigQueryMaterializedViewConfig)

        if new_materialized_view.enable_refresh != existing_materialized_view.enable_refresh:
            config_change_collection.auto_refresh = BigQueryAutoRefreshConfigChange(
                action=RelationConfigChangeAction.alter,
                context=new_materialized_view.enable_refresh,
            )

        if new_materialized_view.cluster_by != existing_materialized_view.cluster_by:
            config_change_collection.cluster_by = BigQueryClusterConfigChange(
                action=RelationConfigChangeAction.alter,
                context=new_materialized_view.cluster_by,
            )

        if new_materialized_view.partition_by != existing_materialized_view.partition_by:
            config_change_collection.partition_by = BigQueryPartitionConfigChange(
                action=RelationConfigChangeAction.alter,
                context=new_materialized_view.partition_by,
            )

        if config_change_collection:
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
