from dbt.tests.adapter.materialized_view.changes import (
    MaterializedViewChanges,
    MaterializedViewChangesApplyMixin,
    MaterializedViewChangesContinueMixin,
    MaterializedViewChangesFailMixin,
)
from dbt.tests.util import get_connection, get_model_file, set_model_file

from dbt.adapters.bigquery.relation_configs import BigQueryMaterializedViewConfig

from tests.functional.adapter.materialized_view_tests._mixin import BigQueryMaterializedViewMixin


class BigQueryMaterializedViewChanges(BigQueryMaterializedViewMixin, MaterializedViewChanges):
    @staticmethod
    def check_start_state(project, materialized_view):
        with get_connection(project.adapter):
            results = project.adapter.describe_relation(materialized_view)
        assert isinstance(results, BigQueryMaterializedViewConfig)
        assert results.options.enable_refresh is True
        assert results.options.refresh_interval_minutes == 60
        assert results.options.max_staleness == "0-0 0 0:45:0"  # ~= "INTERVAL 45 MINUTE"
        assert results.cluster.fields == frozenset({"id", "value"})

    @staticmethod
    def change_config_via_alter(project, materialized_view):
        initial_model = get_model_file(project, materialized_view)
        new_model = initial_model.replace("enable_refresh=True", "enable_refresh=False")
        set_model_file(project, materialized_view, new_model)

    @staticmethod
    def check_state_alter_change_is_applied(project, materialized_view):
        with get_connection(project.adapter):
            results = project.adapter.describe_relation(materialized_view)
        assert isinstance(results, BigQueryMaterializedViewConfig)
        # these change when run manually
        assert results.options.enable_refresh is False
        assert results.options.refresh_interval_minutes == 30  # BQ returns it to the default
        # this does not change when run manually
        # in fact, it doesn't even show up in the DDL whereas the other two do
        assert results.options.max_staleness is None

    @staticmethod
    def change_config_via_replace(project, materialized_view):
        initial_model = get_model_file(project, materialized_view)
        new_model = initial_model.replace('cluster_by=["id", "value"]', 'cluster_by="id"')
        set_model_file(project, materialized_view, new_model)

    @staticmethod
    def check_state_replace_change_is_applied(project, materialized_view):
        with get_connection(project.adapter):
            results = project.adapter.describe_relation(materialized_view)
        assert isinstance(results, BigQueryMaterializedViewConfig)
        assert results.cluster.fields == frozenset({"id"})


class TestBigQueryMaterializedViewChangesApply(
    BigQueryMaterializedViewChanges, MaterializedViewChangesApplyMixin
):
    pass


class TestBigQueryMaterializedViewChangesContinue(
    BigQueryMaterializedViewChanges, MaterializedViewChangesContinueMixin
):
    pass


class TestBigQueryMaterializedViewChangesFail(
    BigQueryMaterializedViewChanges, MaterializedViewChangesFailMixin
):
    pass
