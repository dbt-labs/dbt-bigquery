from typing import List

import pytest

from dbt.tests.util import run_dbt

from tests.functional.adapter.materialized_view_tests import models


RecordSet = List[tuple]


class TestMaterializedView:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "base_table.sql": models.MODEL_BASE_TABLE,
            "mat_view.sql": models.MODEL_MAT_VIEW,
        }

    @staticmethod
    def get_records(project, relation_name) -> RecordSet:
        sql = f"select * from {project.database}.{project.test_schema}.{relation_name}"
        agate_rows = project.run_sql(sql, fetch="all")
        return [tuple(row) for row in agate_rows]

    def test_create_materialized_view(self, project):
        """
        Run dbt once to set up the table and view
        Verify that the view sees the table
        Update the table
        Verify that the view sees the update
        """
        run_dbt()
        records = self.get_records(project, "mat_view")
        assert records == [(1,)]

        sql = (
            f"insert into {project.database}.{project.test_schema}.base_table (my_col) values (2)"
        )
        project.run_sql(sql)

        records = self.get_records(project, "mat_view")
        assert sorted(records) == sorted([(1,), (2,)])
