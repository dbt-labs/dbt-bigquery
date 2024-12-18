import pytest
from google.api_core.exceptions import NotFound
from dbt.adapters.bigquery.relation import BigQueryRelation
from dbt.tests.util import run_dbt, get_connection, relation_from_name


_INCREMENTAL_MODEL = """
{{
    config(
        materialized="incremental",
        on_schema_change="sync_all_columns"
    )
}}
    select 20 as id, cast('2020-01-01 01:00:00' as datetime) as date_hour union all
    select 40 as id, cast('2020-01-01 02:00:00' as datetime) as date_hour
"""

_INCREMENTAL_MODEL_YAML = """version: 2
models:
- name: test_drop_relation
  columns:
  - name: id
    type: int64
  - name: date_hour
    type: datetime
"""


class BaseIncrementalModelConfig:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "test_drop_relation.sql": _INCREMENTAL_MODEL,
            "schema.yml": _INCREMENTAL_MODEL_YAML,
        }


class TestIncrementalModel(BaseIncrementalModelConfig):
    def test_incremental_model_succeeds(self, project):
        """
        Steps:
        1. Create the model
        2. Merge into the model using __dbt_tmp table
        3. Assert raises NotFound exception
        """
        results = run_dbt(["run"])
        assert len(results) == 1
        results = run_dbt(["run"])
        assert len(results) == 1
        relation: BigQueryRelation = relation_from_name(
            project.adapter, "test_drop_relation__dbt_tmp"
        )
        adapter = project.adapter
        with pytest.raises(NotFound):
            with get_connection(project.adapter) as conn:
                conn.handle.get_table(
                    adapter.connections.get_bq_table(
                        relation.database, relation.schema, relation.table
                    )
                )
