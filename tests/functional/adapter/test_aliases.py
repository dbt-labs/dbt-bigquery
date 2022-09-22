import pytest
from dbt.tests.adapter.aliases.test_aliases import TestAliases, TestSameAliasDifferentSchemas

_MACROS__BIGQUERY_CAST_SQL = """

{% macro bigquery__string_literal(s) %}
    cast('{{ s }}' as string)
{% endmacro %}

"""

class TestAliasesBigQuery(TestAliases):
    @pytest.fixture(scope="class")
    def macros(self):
        return {"bigquery_cast.sql": _MACROS__BIGQUERY_CAST_SQL}

    pass

class TestSameTestSameAliasDifferentSchemasBigQuery(TestSameAliasDifferentSchemas):
    @pytest.fixture(scope="class")
    def macros(self):
        return {"bigquery_cast.sql": _MACROS__BIGQUERY_CAST_SQL}

    pass