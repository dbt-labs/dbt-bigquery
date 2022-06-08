import pytest
from dbt.tests.util import run_dbt

# This is to test a edge case found in https://github.com/dbt-labs/dbt-bigquery/pull/165/files

tests__get_cols_in_sql = """
{% test get_cols_in(model) %}

  {# The step which causes the issue #}
  {%- set relation = api.Relation.create(identifier=model.table) if execute -%}

  {% set columns = adapter.get_columns_in_relation(relation) %}

  select
    {% for col in columns %}
      {{ col.name }} {{ "," if not loop.last }}
    {% endfor %}

  from {{ model }}
  limit 0

{% endtest %}
"""

models__my_model = """select 1 as id, 'text' as another_col
"""

properties__model_yml = """
version: 2
models:
  - name: my_model
    tests:
      - get_cols_in
"""

class TestIncompleteRelation:
    @pytest.fixture(scope="class")
    def properties(self):
        return {"properties_model_yml.yml": properties__model_yml}

    @pytest.fixture(scope="class")
    def tests(self):
        return {"tests__get_col_in.sql": tests__get_cols_in_sql}

    @pytest.fixture(scope="class")
    def models(self):
        return { "models__my_model.sql": models__my_model }

    def test_incomplete_relation(self, project):
        run_dbt(["run"])