import pytest
from dbt.tests.adapter.dbt_show.test_dbt_show import BaseShowSqlHeader, BaseShowLimit


my_model_sql_header_sql = """
{{
  config(
    materialized = "table"
  )
}}

{% call set_sql_header(config) %}
DECLARE DEMO STRING DEFAULT 'hello world';
{% endcall %}

SELECT DEMO as column_name
"""


class TestBigQueryShowLimit(BaseShowLimit):
    pass


class TestBigQueryShowSqlHeader(BaseShowSqlHeader):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "sql_header.sql": my_model_sql_header_sql,
        }
