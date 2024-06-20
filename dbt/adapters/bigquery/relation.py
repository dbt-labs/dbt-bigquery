from dataclasses import dataclass, field
from itertools import chain, islice
from typing import FrozenSet, Optional, TypeVar

from dbt.adapters.base.relation import BaseRelation, ComponentName, InformationSchema
from dbt.adapters.contracts.relation import RelationType, RelationConfig
from dbt.adapters.relation_configs import RelationConfigChangeAction
from dbt_common.exceptions import CompilationError
from dbt_common.utils.dict import filter_null_values

from dbt.adapters.bigquery.relation_configs import (
    BigQueryMaterializedViewConfig,
    BigQueryMaterializedViewConfigChangeset,
    BigQueryRelationConfigChange,
)


Self = TypeVar("Self", bound="BigQueryRelation")


@dataclass(frozen=True, eq=False, repr=False)
class BigQueryRelation(BaseRelation):
    quote_character: str = "`"
    location: Optional[str] = None
    require_alias: bool = False

    renameable_relations: FrozenSet[RelationType] = field(
        default_factory=lambda: frozenset(
            {
                RelationType.Table,
            }
        )
    )

    replaceable_relations: FrozenSet[RelationType] = field(
        default_factory=lambda: frozenset(
            {
                RelationType.View,
                RelationType.Table,
            }
        )
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
    def materialized_view_from_relation_config(
        cls, relation_config: RelationConfig
    ) -> BigQueryMaterializedViewConfig:
        return BigQueryMaterializedViewConfig.from_relation_config(relation_config)  # type: ignore

    @classmethod
    def materialized_view_config_changeset(
        cls,
        existing_materialized_view: BigQueryMaterializedViewConfig,
        relation_config: RelationConfig,
    ) -> Optional[BigQueryMaterializedViewConfigChangeset]:
        config_change_collection = BigQueryMaterializedViewConfigChangeset()
        new_materialized_view = cls.materialized_view_from_relation_config(relation_config)

        def add_change(option: str, requires_full_refresh: bool):
            cls._add_change(
                config_change_collection=config_change_collection,
                new_relation=new_materialized_view,
                existing_relation=existing_materialized_view,
                option=option,
                requires_full_refresh=requires_full_refresh,
            )

        add_change("partition", True)
        add_change("cluster", True)
        add_change("enable_refresh", False)
        add_change("refresh_interval_minutes", False)
        add_change("max_staleness", False)
        add_change("allow_non_incremental_definition", True)
        add_change("kms_key_name", False)
        add_change("description", False)
        add_change("labels", False)

        if config_change_collection.has_changes:
            return config_change_collection
        return None

    @classmethod
    def _add_change(
        cls,
        config_change_collection,
        new_relation,
        existing_relation,
        option: str,
        requires_full_refresh: bool,
    ) -> None:
        # if there's no change, don't do anything
        if getattr(new_relation, option) == getattr(existing_relation, option):
            return

        # determine the type of change: drop, create, alter (includes full refresh)
        if getattr(new_relation, option) is None:
            action = RelationConfigChangeAction.drop
        elif getattr(existing_relation, option) is None:
            action = RelationConfigChangeAction.create
        else:
            action = RelationConfigChangeAction.alter

        # don't worry about passing along the context if it's a going to result in a full refresh
        if requires_full_refresh:
            context = None
        else:
            context = getattr(new_relation, option)

        # add the change to the collection for downstream processing
        setattr(
            config_change_collection,
            option,
            BigQueryRelationConfigChange(
                action=action,
                context=context,
                requires_full_refresh=requires_full_refresh,
            ),
        )

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
