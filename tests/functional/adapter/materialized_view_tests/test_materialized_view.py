from typing import Optional, Tuple

import pytest

from dbt.adapters.base.relation import BaseRelation
from dbt.tests.util import get_connection, run_dbt_and_capture
from dbt.tests.adapter.materialized_view.basic import MaterializedViewBasic

from dbt.tests.adapter.materialized_view.files import MY_TABLE, MY_VIEW


MY_MATERIALIZED_VIEW = """
{{ config(
    materialized='materialized_view'
) }}
select * from {{ ref('my_seed') }}
"""


class TestBigqueryMaterializedViewsBasic(MaterializedViewBasic):
    @pytest.fixture(scope="class", autouse=True)
    def models(self):
        yield {
            "my_table.sql": MY_TABLE,
            "my_view.sql": MY_VIEW,
            "my_materialized_view.sql": MY_MATERIALIZED_VIEW,
        }

    @staticmethod
    def insert_record(project, table: BaseRelation, record: Tuple[int, int]):
        my_id, value = record
        project.run_sql(f"insert into {table} (id, value) values ({my_id}, {value})")

    @staticmethod
    def refresh_materialized_view(project, materialized_view: BaseRelation):
        sql = f"call bq.refresh_materialized_view({materialized_view})"
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
                    relation.database, relation.schema, relation.table
                )
            )
        return table.table_type

    def test_materialized_view_create_idempotent(self, project, my_materialized_view):
        # setup creates it once; verify it's there and run once
        assert self.query_relation_type(project, my_materialized_view) == "materialized_view"
        run_dbt_and_capture(["run", "--models", my_materialized_view.identifier])
        assert self.query_relation_type(project, my_materialized_view) == "materialized_view"

    def test_table_replaces_materialized_view(self, project, my_materialized_view):
        super().test_table_replaces_materialized_view(project, my_materialized_view)

    def test_view_replaces_materialized_view(self, project, my_materialized_view):
        super().test_view_replaces_materialized_view(project, my_materialized_view)
