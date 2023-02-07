import pytest
import yaml
from dbt.tests.util import run_dbt
from dbt.tests.adapter.column_types.test_column_types import BaseColumnTypes
from dbt.tests.adapter.column_types.fixtures import macro_test_is_type_sql
from tests.functional.adapter.column_types.fixtures import (
    _MACRO_TEST_ALTER_COLUMN_TYPE,
    _MODEL_ALT_SQL,
    _ALT_SCHEMA_YML
)



class BaseAlterColumnTypes(BaseColumnTypes):

    @pytest.fixture(scope="class")
    def macros(self):
        return {
            "test_is_type.sql": macro_test_is_type_sql,
            "test_alter_column_type.sql": _MACRO_TEST_ALTER_COLUMN_TYPE
        }

    def run_and_alter_and_test(self, alter_column_type_args):
        results = run_dbt(["run"])
        assert len(results) == 1
        run_dbt(['run-operation', 'test_alter_column_type', '--args', alter_column_type_args])
        results = run_dbt(["test"])
        assert len(results) == 1



class TestBigQueryAlterColumnTypes(BaseAlterColumnTypes):

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "model.sql": _MODEL_ALT_SQL,
            "schema.yml": _ALT_SCHEMA_YML
        }

    def test_bigquery_alter_column_types(self, project):
        alter_column_type_args = yaml.safe_dump({
            'model_name': 'model',
            'column_name': 'int64_col',
            'new_column_type': 'string'
        })

        self.run_and_alter_and_test(alter_column_type_args)