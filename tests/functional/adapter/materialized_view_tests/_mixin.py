from typing import Optional, Tuple

import pytest

from dbt.adapters.base.relation import BaseRelation
from dbt.contracts.relation import RelationType
from dbt.tests.adapter.materialized_view.files import MY_TABLE, MY_VIEW
from dbt.tests.util import (
    get_connection,
    get_model_file,
    run_dbt,
    set_model_file,
)

from tests.functional.adapter.materialized_view_tests import _files


class BigQueryMaterializedViewMixin:
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
    def my_other_base_table(self, project) -> BaseRelation:
        """
        Following the sentiment of `my_base_table` above, if we want to alter the partition
        on the materialized view, we either need to update the partition on the base table,
        or we need a second table with a different partition.
        """
        return project.adapter.Relation.create(
            identifier="my_other_base_table",
            schema=project.test_schema,
            database=project.database,
            type=RelationType.Table,
        )

    @pytest.fixture(scope="function", autouse=True)
    def setup(self, project, my_base_table, my_other_base_table, my_materialized_view):  # type: ignore
        run_dbt(["seed"])
        run_dbt(["run", "--full-refresh"])

        # the tests touch these files, store their contents in memory
        initial_model = get_model_file(project, my_materialized_view)

        yield

        # and then reset them after the test runs
        set_model_file(project, my_materialized_view, initial_model)
        project.run_sql(f"drop schema if exists {project.test_schema} cascade")

    @pytest.fixture(scope="class", autouse=True)
    def seeds(self):
        return {"my_seed.csv": _files.MY_SEED}

    @pytest.fixture(scope="class", autouse=True)
    def models(self):
        yield {
            "my_table.sql": MY_TABLE,
            "my_view.sql": MY_VIEW,
            "my_base_table.sql": _files.MY_BASE_TABLE,
            "my_other_base_table.sql": _files.MY_OTHER_BASE_TABLE,
            "my_materialized_view.sql": _files.MY_MATERIALIZED_VIEW,
        }

    @staticmethod
    def insert_record(project, table: BaseRelation, record: Tuple[int, int]) -> None:
        my_id, value = record
        project.run_sql(f"insert into {table} (id, value) values ({my_id}, {value})")

    @staticmethod
    def refresh_materialized_view(project, materialized_view: BaseRelation) -> None:
        sql = f"""
        call bq.refresh_materialized_view(
            '{materialized_view.database}.{materialized_view.schema}.{materialized_view.identifier}'
        )
        """
        project.run_sql(sql)

    @staticmethod
    def query_row_count(project, relation: BaseRelation) -> int:
        sql = f"select count(*) from {relation}"
        return project.run_sql(sql, fetch="one")[0]

    # look into syntax
    @staticmethod
    def query_relation_type(project, relation: BaseRelation) -> Optional[str]:
        with get_connection(project.adapter) as conn:
            table = conn.handle.get_table(
                project.adapter.connections.get_bq_table(
                    relation.database, relation.schema, relation.identifier
                )
            )
        return table.table_type.lower()
