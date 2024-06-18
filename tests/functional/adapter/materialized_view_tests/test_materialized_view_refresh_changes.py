from dbt.adapters.base.relation import BaseRelation
from dbt.adapters.contracts.relation import RelationType
from dbt.tests.util import (
    get_connection,
    get_model_file,
    run_dbt,
    set_model_file,
)
import pytest

from tests.functional.adapter.materialized_view_tests import _files


class RefreshChanges:
    @pytest.fixture(scope="class", autouse=True)
    def seeds(self):
        return {"my_seed.csv": _files.MY_SEED}

    @pytest.fixture(scope="class", autouse=True)
    def models(self):
        yield {
            "my_base_table.sql": _files.MY_BASE_TABLE,
            "my_materialized_view.sql": _files.MY_MATERIALIZED_VIEW_INCREMENTAL,
        }

    @pytest.fixture(scope="class")
    def my_base_table(self, project) -> BaseRelation:
        return project.adapter.Relation.create(
            identifier="my_base_table",
            schema=project.test_schema,
            database=project.database,
            type=RelationType.Table,
        )

    @pytest.fixture(scope="class")
    def my_materialized_view(self, project) -> BaseRelation:
        return project.adapter.Relation.create(
            identifier="my_materialized_view",
            schema=project.test_schema,
            database=project.database,
            type=RelationType.MaterializedView,
        )

    @staticmethod
    def check_start_state(project, my_materialized_view):
        raise NotImplementedError

    @staticmethod
    def change_config(project, my_materialized_view):
        raise NotImplementedError

    @staticmethod
    def check_end_state(project, my_materialized_view):
        raise NotImplementedError

    def test_refresh_changes(self, project, my_materialized_view):
        # arrange
        run_dbt(["seed"])
        run_dbt(["run"])
        self.check_start_state(project, my_materialized_view)

        # act
        self.change_config(project, my_materialized_view)
        run_dbt(["run", "--models", my_materialized_view.identifier])

        # assert
        self.check_end_state(project, my_materialized_view)


class TestTurnOnAllowNonIncremental(RefreshChanges):
    @pytest.fixture(scope="class", autouse=True)
    def models(self):
        yield {
            "my_base_table.sql": _files.MY_BASE_TABLE,
            "my_materialized_view.sql": _files.MY_MINIMAL_MATERIALIZED_VIEW,
        }

    @staticmethod
    def check_start_state(project, my_materialized_view):
        with get_connection(project.adapter):
            results = project.adapter.describe_relation(my_materialized_view)
        assert results.options.enable_refresh is True
        assert results.options.refresh_interval_minutes == 30
        assert results.options.max_staleness is None
        assert results.options.allow_non_incremental_definition is None

    @staticmethod
    def change_config(project, my_materialized_view):
        initial_model = get_model_file(project, my_materialized_view)
        new_model = initial_model.replace(
            "materialized='materialized_view'",
            """materialized='materialized_view', allow_non_incremental_definition=True, max_staleness="INTERVAL '0-0 0 0:45:0' YEAR TO SECOND" """,
        )
        set_model_file(project, my_materialized_view, new_model)

    @staticmethod
    def check_end_state(project, my_materialized_view):
        with get_connection(project.adapter):
            results = project.adapter.describe_relation(my_materialized_view)
        assert results.options.enable_refresh is True
        assert results.options.refresh_interval_minutes == 30
        assert results.options.max_staleness == "INTERVAL '0-0 0 0:45:0' YEAR TO SECOND"
        assert results.options.allow_non_incremental_definition is True


class TestTurnOffAllowNonIncremental(RefreshChanges):
    @pytest.fixture(scope="class", autouse=True)
    def models(self):
        yield {
            "my_base_table.sql": _files.MY_BASE_TABLE,
            "my_materialized_view.sql": _files.MY_MATERIALIZED_VIEW_NON_INCREMENTAL,
        }

    @staticmethod
    def check_start_state(project, my_materialized_view):
        with get_connection(project.adapter):
            results = project.adapter.describe_relation(my_materialized_view)
        assert results.options.enable_refresh is True
        assert results.options.refresh_interval_minutes == 30
        assert results.options.max_staleness == "INTERVAL '0-0 0 0:45:0' YEAR TO SECOND"
        assert results.options.allow_non_incremental_definition is True

    @staticmethod
    def change_config(project, my_materialized_view):
        initial_model = get_model_file(project, my_materialized_view)
        new_model = initial_model.replace(
            "allow_non_incremental_definition=True", "allow_non_incremental_definition=False"
        )
        set_model_file(project, my_materialized_view, new_model)

    @staticmethod
    def check_end_state(project, my_materialized_view):
        with get_connection(project.adapter):
            results = project.adapter.describe_relation(my_materialized_view)
        assert results.options.enable_refresh is True
        assert results.options.refresh_interval_minutes == 30
        assert results.options.max_staleness == "INTERVAL '0-0 0 0:45:0' YEAR TO SECOND"
        assert results.options.allow_non_incremental_definition is False


class TestUnsetAllowNonIncremental(RefreshChanges):
    @pytest.fixture(scope="class", autouse=True)
    def models(self):
        yield {
            "my_base_table.sql": _files.MY_BASE_TABLE,
            "my_materialized_view.sql": _files.MY_MATERIALIZED_VIEW_NON_INCREMENTAL,
        }

    @staticmethod
    def check_start_state(project, my_materialized_view):
        with get_connection(project.adapter):
            results = project.adapter.describe_relation(my_materialized_view)
        assert results.options.enable_refresh is True
        assert results.options.refresh_interval_minutes == 30
        assert results.options.max_staleness == "INTERVAL '0-0 0 0:45:0' YEAR TO SECOND"
        assert results.options.allow_non_incremental_definition is True

    @staticmethod
    def change_config(project, my_materialized_view):
        initial_model = get_model_file(project, my_materialized_view)
        new_model = initial_model.replace("allow_non_incremental_definition=True,", "")
        set_model_file(project, my_materialized_view, new_model)

    @staticmethod
    def check_end_state(project, my_materialized_view):
        with get_connection(project.adapter):
            results = project.adapter.describe_relation(my_materialized_view)
        assert results.options.enable_refresh is True
        assert results.options.refresh_interval_minutes == 30
        assert results.options.max_staleness == "INTERVAL '0-0 0 0:45:0' YEAR TO SECOND"
        assert results.options.allow_non_incremental_definition is None


class TestTurnOffRefresh(RefreshChanges):
    @pytest.fixture(scope="class", autouse=True)
    def models(self):
        yield {
            "my_base_table.sql": _files.MY_BASE_TABLE,
            "my_materialized_view.sql": _files.MY_MATERIALIZED_VIEW_REFRESH_CONFIG,
        }

    @staticmethod
    def check_start_state(project, my_materialized_view):
        with get_connection(project.adapter):
            results = project.adapter.describe_relation(my_materialized_view)
        assert results.options.enable_refresh is True
        assert results.options.refresh_interval_minutes == 60
        assert results.options.max_staleness == "INTERVAL '0-0 0 0:45:0' YEAR TO SECOND"
        assert results.options.allow_non_incremental_definition is True

    @staticmethod
    def change_config(project, my_materialized_view):
        initial_model = get_model_file(project, my_materialized_view)
        new_model = initial_model.replace("enable_refresh=True", "enable_refresh=False")
        set_model_file(project, my_materialized_view, new_model)

    @staticmethod
    def check_end_state(project, my_materialized_view):
        with get_connection(project.adapter):
            results = project.adapter.describe_relation(my_materialized_view)
        assert results.options.enable_refresh is False
        assert (
            results.options.refresh_interval_minutes == 30
        )  # this is a defaulted value in BQ, so it doesn't get cleared
        assert results.options.max_staleness is None
        assert results.options.allow_non_incremental_definition is None
