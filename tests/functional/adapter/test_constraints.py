import pytest
from dbt.tests.util import relation_from_name
from dbt.tests.adapter.constraints.test_constraints import (
    BaseConstraintsColumnsEqual,
    BaseConstraintsRuntimeEnforcement
)
from dbt.tests.adapter.constraints.fixtures import (
    my_model_sql,
    my_model_wrong_order_sql,
    my_model_wrong_name_sql,
    model_schema_yml,
)

_expected_sql_bigquery = """
create or replace table {0} (
    id integer  not null    ,
    color string  ,
    date_day date
)
OPTIONS()
as (
    select
        1 as id,
        'blue' as color,
        cast('2019-01-01' as date) as date_day
);
"""

# Different on BigQuery:
# - does not support a data type named 'text' (TODO handle this via type translation/aliasing!)
# - raises an explicit error, if you try to set a primary key constraint, because it's not enforced
constraints_yml = model_schema_yml.replace("text", "string").replace("primary key", "")

class TestBigQueryConstraintsColumnsEqual(BaseConstraintsColumnsEqual):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "my_model_wrong_order.sql": my_model_wrong_order_sql,
            "my_model_wrong_name.sql": my_model_wrong_name_sql,
            "constraints_schema.yml": constraints_yml,
        }


class TestBigQueryConstraintsRuntimeEnforcement(BaseConstraintsRuntimeEnforcement):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "my_model.sql": my_model_sql,
            "constraints_schema.yml": constraints_yml,
        }
    
    @pytest.fixture(scope="class")
    def expected_sql(self, project):
        relation = relation_from_name(project.adapter, "my_model")
        return _expected_sql_bigquery.format(relation)

    @pytest.fixture(scope="class")
    def expected_error_messages(self):
        return ["Required field id cannot be null"]
