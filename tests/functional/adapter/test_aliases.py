import pytest
from dbt.tests.adapter.aliases.test_aliases import BaseAliases, BaseSameAliasDifferentSchemas

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


class TestSameTestSameAliasDifferentSchemasBigQuery(BaseSameAliasDifferentSchemas):
    @pytest.fixture(scope="class")
    def macros(self):
        return {"bigquery_cast.sql": MACROS__BIGQUERY_CAST_SQL, "expect_value.sql": MACROS__EXPECT_VALUE_SQL}
