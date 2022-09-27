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

class TestAliasesBigQuery(BaseAliases):
    @pytest.fixture(scope="class")
    def macros(self):
        return {"bigquery_cast.sql": MACROS__BIGQUERY_CAST_SQL, "expect_value.sql": MACROS__EXPECT_VALUE_SQL}


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
        return {"bigquery_cast.sql": MACROS__BIGQUERY_CAST_SQL, "expect_value.sql": MACROS__EXPECT_VALUE_SQL}
