import pytest

from dbt.tests.util import run_dbt
from dbt.tests.adapter.materialized_view.basic import MaterializedViewBasic
from dbt.tests.util import get_model_file, set_model_file

from tests.functional.adapter.materialized_view_tests._mixin import BigQueryMaterializedViewMixin
from tests.functional.adapter.materialized_view_tests import _files


class TestBigqueryMaterializedViewsBasic(BigQueryMaterializedViewMixin, MaterializedViewBasic):
    @pytest.fixture(scope="function", autouse=True)
    def setup(self, project, my_materialized_view):  # type: ignore
        run_dbt(["seed"])
        run_dbt(["run", "--full-refresh"])

        # the tests touch these files, store their contents in memory
        initial_model = get_model_file(project, my_materialized_view)

        yield

        # and then reset them after the test runs
        set_model_file(project, my_materialized_view, initial_model)
        project.run_sql(f"drop schema if exists {project.test_schema} cascade")

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


class TestMaterializedViewRerun:
    """
    This addresses: https://github.com/dbt-labs/dbt-bigquery/issues/1007

    This effectively tests that defaults get properly set so that the run is idempotent.
    If the defaults are not properly set, changes could appear when there are no changes
    and cause unexpected scenarios.
    """

    @pytest.fixture(scope="function", autouse=True)
    def setup(self, project, my_materialized_view):  # type: ignore
        run_dbt(["seed"])
        run_dbt(["run", "--full-refresh"])

        # the tests touch these files, store their contents in memory
        initial_model = get_model_file(project, my_materialized_view)

        yield

        # and then reset them after the test runs
        set_model_file(project, my_materialized_view, initial_model)
        project.run_sql(f"drop schema if exists {project.test_schema} cascade")

    @pytest.fixture(scope="class", autouse=True)
    def models(self):
        return {"my_minimal_materialized_view.sql": _files.MY_MINIMAL_MATERIALIZED_VIEW}

    @pytest.fixture(scope="class", autouse=True)
    def seeds(self):
        return {"my_seed.csv": _files.MY_SEED}

    def test_minimal_materialized_view_is_idempotent(self, project):
        run_dbt(["seed"])
        run_dbt(["run"])
        run_dbt(["run"])
