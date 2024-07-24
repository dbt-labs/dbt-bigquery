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
        assert results.enable_refresh is True
        assert results.refresh_interval_minutes == 60
        assert results.max_staleness == "INTERVAL '0-0 0 0:45:0' YEAR TO SECOND"
        assert results.allow_non_incremental_definition is True
        assert results.partition.field == "record_valid_date"
        assert results.partition.data_type == "datetime"
        assert results.partition.granularity == "day"
        assert results.cluster.fields == frozenset({"id", "value"})

    @staticmethod
    def change_config_via_alter(project, materialized_view):
        initial_model = get_model_file(project, materialized_view)
        new_model = initial_model.replace(
            "refresh_interval_minutes=60", "refresh_interval_minutes=45"
        )
        set_model_file(project, materialized_view, new_model)

    @staticmethod
    def check_state_alter_change_is_applied(project, materialized_view):
        with get_connection(project.adapter):
            results = project.adapter.describe_relation(materialized_view)
        assert isinstance(results, BigQueryMaterializedViewConfig)
        assert results.options.refresh_interval_minutes == 45

    @staticmethod
    def change_config_via_replace(project, materialized_view):
        initial_model = get_model_file(project, materialized_view)
        # the whitespace to the left on partition matters here
        old_partition = """
    partition_by={
        "field": "record_valid_date",
        "data_type": "datetime",
        "granularity": "day"
    },"""
        new_partition = """
    partition_by={
        "field": "value",
        "data_type": "int64",
        "range": {
            "start": 0,
            "end": 500,
            "interval": 50
        }
    },"""
        new_model = (
            initial_model.replace(old_partition, new_partition)
            .replace("'my_base_table'", "'my_other_base_table'")
            .replace('cluster_by=["id", "value"]', 'cluster_by="id"')
            .replace(
                "allow_non_incremental_definition=True", "allow_non_incremental_definition=False"
            )
        )
        set_model_file(project, materialized_view, new_model)

    @staticmethod
    def check_state_replace_change_is_applied(project, materialized_view):
        with get_connection(project.adapter):
            results = project.adapter.describe_relation(materialized_view)
        assert isinstance(results, BigQueryMaterializedViewConfig)
        assert results.partition.field == "value"
        assert results.partition.data_type == "int64"
        assert results.partition.range == {"start": 0, "end": 500, "interval": 50}
        assert results.cluster.fields == frozenset({"id"})
        assert results.allow_non_incremental_definition is False


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
