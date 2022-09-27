import pytest
import os
import dbt.tests.adapter.aliases.test_aliases as dbt_tests

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

class TestAliasesBigQuery(dbt_tests.BaseAliases):
    @pytest.fixture(scope="class")
    def macros(self):
        return {"bigquery_cast.sql": MACROS__BIGQUERY_CAST_SQL, "expect_value.sql": MACROS__EXPECT_VALUE_SQL}


class TestSameTestSameAliasDifferentDatabasesBigQuery(dbt_tests.BaseSameAliasDifferentDatabases):
    @pytest.fixture(scope="class")
    def macros(self):
        return {"bigquery_cast.sql": MACROS__BIGQUERY_CAST_SQL, "expect_value.sql": MACROS__EXPECT_VALUE_SQL}
