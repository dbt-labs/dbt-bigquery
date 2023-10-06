import pytest

from dbt.adapters.base.relation import BaseRelation
from dbt.contracts.relation import RelationType
from dbt.tests.util import get_connection, run_dbt

from dbt.adapters.bigquery.relation_configs import BigQueryMaterializedViewConfig
from tests.functional.adapter.describe_relation._files import (
    MY_BASE_TABLE,
    MY_MATERIALIZED_VIEW,
    MY_SEED,
)


class TestDescribeRelation:
    @pytest.fixture(scope="class", autouse=True)
    def seeds(self):
        return {"my_seed.csv": MY_SEED}

    @pytest.fixture(scope="class", autouse=True)
    def models(self):
        yield {
            "my_base_table.sql": MY_BASE_TABLE,
            "my_materialized_view.sql": MY_MATERIALIZED_VIEW,
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
        assert results.materialized_view_name == f'"{my_materialized_view.identifier}"'
        assert results.schema_name == f'"{my_materialized_view.schema}"'
        assert results.database_name == f'"{my_materialized_view.database}"'
        assert results.cluster.fields == frozenset({"id"})
        assert results.auto_refresh.enable_refresh is True
        assert results.auto_refresh.refresh_interval_minutes == 30
