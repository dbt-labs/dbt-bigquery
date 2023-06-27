import pytest
from dbt.tests.adapter.constraints.test_constraints import (
    BaseTableConstraintsColumnsEqual,
    BaseViewConstraintsColumnsEqual,
    BaseIncrementalConstraintsColumnsEqual,
    BaseTableContractSqlHeader,
    BaseIncrementalContractSqlHeader,
    BaseConstraintsRuntimeDdlEnforcement,
    BaseConstraintsRollback,
    BaseIncrementalConstraintsRuntimeDdlEnforcement,
    BaseIncrementalConstraintsRollback,
    BaseModelConstraintsRuntimeEnforcement,
    BaseConstraintQuotedColumn,
)
from dbt.tests.adapter.constraints.fixtures import (
    my_model_sql,
    my_incremental_model_sql,
    my_model_wrong_order_sql,
    my_model_view_wrong_order_sql,
    my_model_incremental_wrong_order_sql,
    my_model_wrong_name_sql,
    my_model_view_wrong_name_sql,
    my_model_incremental_wrong_name_sql,
    my_model_with_quoted_column_name_sql,
    model_schema_yml,
    constrained_model_schema_yml,
    model_contract_header_schema_yml,
    model_quoted_column_schema_yml,
    model_fk_constraint_schema_yml,
    my_model_wrong_order_depends_on_fk_sql,
    foreign_key_model_sql,
    my_model_incremental_wrong_order_depends_on_fk_sql,
)

from tests.functional.adapter.constraints.fixtures import (
    my_model_struct_wrong_data_type_sql,
    my_model_struct_correct_data_type_sql,
    my_model_double_struct_wrong_data_type_sql,
    my_model_double_struct_correct_data_type_sql,
    model_struct_data_type_schema_yml,
    model_double_struct_data_type_schema_yml,
    my_model_struct_sql,
    model_struct_schema_yml,
)

from dbt.tests.util import run_dbt_and_capture, run_dbt

_expected_sql_bigquery = """
create or replace table <model_identifier> (
    id integer not null primary key not enforced references <foreign_key_model_identifier> (id) not enforced,
    color string,
    date_day string
)
OPTIONS()
as (
    select id,
    color,
    date_day from
  (
    -- depends_on: <foreign_key_model_identifier>
    select 'blue' as color,
    1 as id,
    '2019-01-01' as date_day
  ) as model_subq
);
"""

_expected_struct_sql_bigquery = """
create or replace table <model_identifier> (
    id struct<nested_column string not null, nested_column2 string>
)
OPTIONS()
as (
    select id from
  (
    select STRUCT("test" as nested_column, "test" as nested_column2) as id
  ) as model_subq
);
"""

# Different on BigQuery:
# - does not support a data type named 'text' (TODO handle this via type translation/aliasing!)
constraints_yml = model_schema_yml.replace("text", "string")
model_constraints_yml = constrained_model_schema_yml.replace("text", "string")
model_contract_header_schema_yml = model_contract_header_schema_yml.replace("text", "string")
model_fk_constraint_schema_yml = model_fk_constraint_schema_yml.replace("text", "string")
constrained_model_schema_yml = constrained_model_schema_yml.replace("text", "string")

my_model_contract_sql_header_sql = """
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

my_model_incremental_contract_sql_header_sql = """
{{
  config(
    materialized = "incremental",
    on_schema_change="append_new_columns"
  )
}}

{% call set_sql_header(config) %}
DECLARE DEMO STRING DEFAULT 'hello world';
{% endcall %}

SELECT DEMO as column_name
"""


class BigQueryColumnEqualSetup:
    @pytest.fixture
    def string_type(self):
        return "STRING"

    @pytest.fixture
    def int_type(self):
        return "INT64"

    @pytest.fixture
    def data_types(self, int_type, string_type):
        # sql_column_value, schema_data_type, error_data_type
        return [
            ["1", int_type, int_type],
            ["'1'", string_type, string_type],
            ["cast('2019-01-01' as date)", "date", "DATE"],
            ["true", "bool", "BOOL"],
            ["cast('2013-11-03 00:00:00-07' as TIMESTAMP)", "timestamp", "TIMESTAMP"],
            ["['a','b','c']", f"ARRAY<{string_type}>", f"ARRAY<{string_type}>"],
            ["[1,2,3]", f"ARRAY<{int_type}>", f"ARRAY<{int_type}>"],
            ["cast(1 as NUMERIC)", "numeric", "NUMERIC"],
            ["""JSON '{"name": "Cooper", "forname": "Alice"}'""", "json", "JSON"],
        ]


class TestBigQueryTableConstraintsColumnsEqual(
    BigQueryColumnEqualSetup, BaseTableConstraintsColumnsEqual
):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "my_model_wrong_order.sql": my_model_wrong_order_sql,
            "my_model_wrong_name.sql": my_model_wrong_name_sql,
            "constraints_schema.yml": constraints_yml,
        }


class TestBigQueryViewConstraintsColumnsEqual(
    BigQueryColumnEqualSetup, BaseViewConstraintsColumnsEqual
):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "my_model_wrong_order.sql": my_model_view_wrong_order_sql,
            "my_model_wrong_name.sql": my_model_view_wrong_name_sql,
            "constraints_schema.yml": constraints_yml,
        }


class TestBigQueryIncrementalConstraintsColumnsEqual(
    BigQueryColumnEqualSetup, BaseIncrementalConstraintsColumnsEqual
):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "my_model_wrong_order.sql": my_model_incremental_wrong_order_sql,
            "my_model_wrong_name.sql": my_model_incremental_wrong_name_sql,
            "constraints_schema.yml": constraints_yml,
        }


class TestBigQueryTableContractsSqlHeader(BaseTableContractSqlHeader):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "my_model_contract_sql_header.sql": my_model_contract_sql_header_sql,
            "constraints_schema.yml": model_contract_header_schema_yml,
        }


class TestBigQueryIncrementalContractsSqlHeader(BaseIncrementalContractSqlHeader):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "my_model_contract_sql_header.sql": my_model_incremental_contract_sql_header_sql,
            "constraints_schema.yml": model_contract_header_schema_yml,
        }


class BaseStructContract:
    @pytest.fixture
    def wrong_schema_data_type(self):
        return "INT64"

    @pytest.fixture
    def correct_schema_data_type(self):
        return "STRING"

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "contract_struct_schema.yml": model_struct_data_type_schema_yml,
            "contract_struct_wrong.sql": my_model_struct_wrong_data_type_sql,
            "contract_struct_correct.sql": my_model_struct_correct_data_type_sql,
        }

    def test__struct_contract_wrong_data_type(
        self, project, correct_schema_data_type, wrong_schema_data_type
    ):
        results, log_output = run_dbt_and_capture(
            ["run", "-s", "contract_struct_wrong"], expect_pass=False
        )
        assert len(results) == 1
        assert results[0].node.config.contract.enforced

        expected = [
            "struct_column_being_tested",
            wrong_schema_data_type,
            correct_schema_data_type,
            "data type mismatch",
        ]
        assert all([(exp in log_output or exp.upper() in log_output) for exp in expected])

    def test__struct_contract_correct_data_type(self, project):
        results = run_dbt(["run", "-s", "contract_struct_correct"])

        assert len(results) == 1
        assert results[0].node.config.contract.enforced


class TestBigQueryStructContract(BaseStructContract):
    pass


class TestBigQueryDoubleStructContract(BaseStructContract):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "contract_struct_schema.yml": model_double_struct_data_type_schema_yml,
            "contract_struct_wrong.sql": my_model_double_struct_wrong_data_type_sql,
            "contract_struct_correct.sql": my_model_double_struct_correct_data_type_sql,
        }


class TestBigQueryTableConstraintsRuntimeDdlEnforcement(BaseConstraintsRuntimeDdlEnforcement):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "my_model.sql": my_model_wrong_order_depends_on_fk_sql,
            "foreign_key_model.sql": foreign_key_model_sql,
            "constraints_schema.yml": model_fk_constraint_schema_yml,
        }

    @pytest.fixture(scope="class")
    def expected_sql(self, project):
        return _expected_sql_bigquery


class TestBigQueryStructTableConstraintsRuntimeDdlEnforcement(
    BaseConstraintsRuntimeDdlEnforcement
):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "my_model.sql": my_model_struct_sql,
            "constraints_schema.yml": model_struct_schema_yml,
        }

    @pytest.fixture(scope="class")
    def expected_sql(self, project):
        return _expected_struct_sql_bigquery


class TestBigQueryTableConstraintsRollback(BaseConstraintsRollback):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "my_model.sql": my_model_sql,
            "constraints_schema.yml": constraints_yml,
        }

    @pytest.fixture(scope="class")
    def expected_error_messages(self):
        return ["Required field id cannot be null"]


class TestBigQueryIncrementalConstraintsRuntimeDdlEnforcement(
    BaseIncrementalConstraintsRuntimeDdlEnforcement
):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "my_model.sql": my_model_incremental_wrong_order_depends_on_fk_sql,
            "foreign_key_model.sql": foreign_key_model_sql,
            "constraints_schema.yml": model_fk_constraint_schema_yml,
        }

    @pytest.fixture(scope="class")
    def expected_sql(self, project):
        return _expected_sql_bigquery


class TestBigQueryIncrementalConstraintsRollback(BaseIncrementalConstraintsRollback):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "my_model.sql": my_incremental_model_sql,
            "constraints_schema.yml": constraints_yml,
        }

    @pytest.fixture(scope="class")
    def expected_error_messages(self):
        return ["Required field id cannot be null"]


class TestBigQueryModelConstraintsRuntimeEnforcement(BaseModelConstraintsRuntimeEnforcement):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "my_model.sql": my_model_wrong_order_depends_on_fk_sql,
            "foreign_key_model.sql": foreign_key_model_sql,
            "constraints_schema.yml": constrained_model_schema_yml,
        }

    @pytest.fixture(scope="class")
    def expected_sql(self):
        return """
create or replace table <model_identifier> (
    id integer not null,
    color string,
    date_day string,
    primary key (id) not enforced,
    foreign key (id) references <foreign_key_model_identifier> (id) not enforced
)
OPTIONS()
as (
    select id,
    color,
    date_day from
  (
    -- depends_on: <foreign_key_model_identifier>
    select
    'blue' as color,
    1 as id,
    '2019-01-01' as date_day
  ) as model_subq
);
"""


class TestBigQueryConstraintQuotedColumn(BaseConstraintQuotedColumn):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "my_model.sql": my_model_with_quoted_column_name_sql,
            "constraints_schema.yml": model_quoted_column_schema_yml.replace("text", "string"),
        }

    @pytest.fixture(scope="class")
    def expected_sql(self):
        return """
create or replace table <model_identifier> (
    id integer not null,
    `from` string not null,
    date_day string
)
options()
as (
    select id, `from`, date_day
    from (
        select
          'blue' as `from`,
          1 as id,
          '2019-01-01' as date_day
    ) as model_subq
);
"""
