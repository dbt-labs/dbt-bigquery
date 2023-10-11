import pytest

from dbt.adapters.base.relation import BaseRelation
from dbt.contracts.relation import RelationType
from dbt.tests.util import get_connection, run_dbt

from dbt.adapters.bigquery.relation_configs import BigQueryMaterializedViewConfig
from tests.functional.adapter.describe_relation import _files


class TestDescribeRelation:
    @pytest.fixture(scope="class", autouse=True)
    def seeds(self):
        return {"my_seed.csv": _files.MY_SEED}

    @pytest.fixture(scope="class", autouse=True)
    def models(self):
        yield {
            "my_base_table.sql": _files.MY_BASE_TABLE,
            "my_materialized_view.sql": _files.MY_MATERIALIZED_VIEW,
            "my_other_base_table.sql": _files.MY_OTHER_BASE_TABLE,
            "my_other_materialized_view.sql": _files.MY_OTHER_MATERIALIZED_VIEW,
        }

    @pytest.fixture(scope="class")
    def my_seed(self, project) -> BaseRelation:
        return project.adapter.Relation.create(
            identifier="my_seed",
            schema=project.test_schema,
            database=project.database,
            type=RelationType.Table,
        )

    @pytest.fixture(scope="class")
    def my_base_table(self, project) -> BaseRelation:
        """
        The base table for a materialized view needs to be partitioned in
        the same way as the materialized view. So if we want to create a partitioned
        materialized view, we need to partition the base table. This table is a
        select * on the seed table, plus a partition.
        """
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

    @pytest.fixture(scope="class")
    def my_other_materialized_view(self, project) -> BaseRelation:
        return project.adapter.Relation.create(
            identifier="my_other_materialized_view",
            schema=project.test_schema,
            database=project.database,
            type=RelationType.MaterializedView,
        )

    @pytest.fixture(scope="class", autouse=True)
    def setup(self, project, my_base_table, my_materialized_view):
        run_dbt(["seed"])
        run_dbt(["run"])
        yield
        project.run_sql(f"drop schema if exists {project.test_schema} cascade")

    def test_describe_materialized_view(self, project, my_materialized_view):
        with get_connection(project.adapter):
            results = project.adapter.describe_relation(my_materialized_view)
        assert isinstance(results, BigQueryMaterializedViewConfig)
        assert results.table_id == f'"{my_materialized_view.identifier}"'
        assert results.dataset_id == f'"{my_materialized_view.schema}"'
        assert results.project_id == f'"{my_materialized_view.database}"'
        assert results.partition.field == "record_date"
        assert results.partition.data_type == "datetime"
        assert results.partition.granularity == "day"
        assert results.cluster.fields == frozenset({"id"})
        assert results.options.enable_refresh is True
        assert results.options.refresh_interval_minutes == 30

    def test_describe_other_materialized_view(self, project, my_other_materialized_view):
        with get_connection(project.adapter):
            results = project.adapter.describe_relation(my_other_materialized_view)
        assert isinstance(results, BigQueryMaterializedViewConfig)
        assert results.table_id == f'"{my_other_materialized_view.identifier}"'
        assert results.dataset_id == f'"{my_other_materialized_view.schema}"'
        assert results.project_id == f'"{my_other_materialized_view.database}"'
        assert results.partition.field == "value"
        assert results.partition.data_type == "int64"
        assert results.partition.range == {"start": 0, "end": 500, "interval": 50}
        assert results.cluster.fields == frozenset({"id"})
        assert results.options.enable_refresh is False
        assert results.options.refresh_interval_minutes == 30  # BQ returns it to the default
