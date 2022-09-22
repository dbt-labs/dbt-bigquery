import pytest
from dbt.tests.adapter.aliases.test_aliases import BaseAliases, BaseSameAliasDifferentSchemas

_MACROS__BIGQUERY_CAST_SQL = """

{% macro bigquery__string_literal(s) %}
    cast('{{ s }}' as string)
{% endmacro %}

"""

class TestAliasesBigQuery(BaseAliases):
    @pytest.fixture(scope="class")
    def macros(self):
        return {"bigquery_cast.sql": _MACROS__BIGQUERY_CAST_SQL}

    pass

class TestSameTestSameAliasDifferentSchemasBigQuery(BaseSameAliasDifferentSchemas):
    @pytest.fixture(scope="class")
    def macros(self):
        return {"bigquery_cast.sql": _MACROS__BIGQUERY_CAST_SQL}

    pass