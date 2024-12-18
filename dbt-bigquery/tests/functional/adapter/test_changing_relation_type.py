from dbt.tests.adapter.relations.test_changing_relation_type import BaseChangeRelationTypeValidator


class TestBigQueryChangeRelationTypes(BaseChangeRelationTypeValidator):
    def test_changing_materialization_changes_relation_type(self, project):
        self._run_and_check_materialization("view")
        self._run_and_check_materialization("table", extra_args=["--full-refresh"])
        self._run_and_check_materialization("view", extra_args=["--full-refresh"])
        self._run_and_check_materialization("incremental", extra_args=["--full-refresh"])
