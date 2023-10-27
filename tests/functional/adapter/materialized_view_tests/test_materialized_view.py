import pytest

from dbt.tests.util import run_dbt
from dbt.tests.adapter.materialized_view.basic import MaterializedViewBasic

from tests.functional.adapter.materialized_view_tests._mixin import BigQueryMaterializedViewMixin


class TestBigqueryMaterializedViewsBasic(BigQueryMaterializedViewMixin, MaterializedViewBasic):
    def test_view_replaces_materialized_view(self, project, my_materialized_view):
        """
        We don't support replacing a view with another object in dbt-bigquery unless we use --full-refresh
        """
        run_dbt(["run", "--models", my_materialized_view.identifier])
        assert self.query_relation_type(project, my_materialized_view) == "materialized_view"

        self.swap_materialized_view_to_view(project, my_materialized_view)

        # add --full-refresh
        run_dbt(["run", "--models", my_materialized_view.identifier, "--full-refresh"])
        assert self.query_relation_type(project, my_materialized_view) == "view"

    @pytest.mark.skip(
        "It looks like BQ updates the materialized view almost immediately, which fails this test."
    )
    def test_materialized_view_only_updates_after_refresh(
        self, project, my_materialized_view, my_seed
    ):
        pass
