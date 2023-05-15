import pytest
import os
from dbt.tests.adapter.aliases.test_aliases import BaseAliases, BaseSameAliasDifferentDatabases

MACROS__BIGQUERY_CAST_SQL = """
{% macro bigquery__string_literal(s) %}
    cast('{{ s }}' as string)
{% endmacro %}
"""

MACROS__EXPECT_VALUE_SQL = """
-- cross-db compatible test, similar to accepted_values

{% test expect_value(model, field, value) %}

select *
from {{ model }}
where {{ field }} != '{{ value }}'

{% endtest %}
"""

MODELS_DUPE_CUSTOM_DATABASE_A = """
select {{ string_literal(this.name) }} as tablename
"""

MODELS_DUPE_CUSTOM_DATABASE_B = """
select {{ string_literal(this.name) }} as tablename
"""

MODELS_SCHEMA_YML = """
version: 2
models:
- name: model_a
  tests:
  - expect_value:
      field: tablename
      value: duped_alias
- name: model_b
  tests:
  - expect_value:
      field: tablename
      value: duped_alias
"""


class TestAliasesBigQuery(BaseAliases):
    @pytest.fixture(scope="class")
    def macros(self):
        return {
            "bigquery_cast.sql": MACROS__BIGQUERY_CAST_SQL,
            "expect_value.sql": MACROS__EXPECT_VALUE_SQL,
        }


class TestSameTestSameAliasDifferentDatabasesBigQuery(BaseSameAliasDifferentDatabases):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "config-version": 2,
            "macro-paths": ["macros"],
            "models": {
                "test": {
                    "alias": "duped_alias",
                    "model_b": {"database": os.getenv("BIGQUERY_TEST_ALT_DATABASE")},
                },
            },
        }

    @pytest.fixture(scope="class")
    def macros(self):
        return {
            "bigquery_cast.sql": MACROS__BIGQUERY_CAST_SQL,
            "expect_value.sql": MACROS__EXPECT_VALUE_SQL,
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "schema.yml": MODELS_SCHEMA_YML,
            "model_a.sql": MODELS_DUPE_CUSTOM_DATABASE_A,
            "model_b.sql": MODELS_DUPE_CUSTOM_DATABASE_B,
        }

    @pytest.fixture(autouse=True)
    def clean_up(self, project):
        yield
        with project.adapter.connection_named("__test"):
            relation = project.adapter.Relation.create(
                database=os.getenv("BIGQUERY_TEST_ALT_DATABASE"), schema=project.test_schema
            )
            project.adapter.drop_schema(relation)
