import pytest
from dbt.tests.util import (
    relation_from_name,
    get_connection,
    run_dbt
)

from dbt.adapters.bigquery import BigQueryRelation

_FIELD_DESCRIPTION_MODEL = """{{
  config(
    materialized='table',
    persist_docs={ 'columns': true }
  )
}}

select
  1 field
"""
_FIELD_DESCRIPTION_MODEL_NAME = "field_description_model"
_FIELD_DESCRIPTION = 'this is not a field'
_FIELD_DESCRIPTION_MODEL_YML = """
version: 2

models:
- name: field_description_model
  columns:
  - name: field
    description: '{{ var("field_description") }}'
"""

class TestBigqueryUpdateColumnDescription:
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            'config-version': 2,
            'vars': {
                'field_description': _FIELD_DESCRIPTION
            }
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {
            f"{_FIELD_DESCRIPTION_MODEL_NAME}.sql": _FIELD_DESCRIPTION_MODEL,
            "schema.yml": _FIELD_DESCRIPTION_MODEL_YML
        }

    def test_bigquery_update_column_description(self, project):
        results = run_dbt(['run'])
        assert len(results) == 1
        relation: BigQueryRelation = relation_from_name(project.adapter, _FIELD_DESCRIPTION_MODEL_NAME)
        adapter = project.adapter
        with get_connection(project.adapter) as conn:
            table = conn.handle.get_table(
                adapter.connections.get_bq_table(relation.database, relation.schema, relation.table))
            for schema_field in table.schema:
                assert schema_field.description == _FIELD_DESCRIPTION
