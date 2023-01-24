import pytest
from dbt.tests.adapter.column_types.test_column_types import BaseColumnTypes

_MODEL_SQL = """
select
    CAST(1 as int64) as int64_col,
    CAST(2.0 as float64) as float64_col,
    CAST(3.0 as numeric) as numeric_col,
    CAST('3' as string) as string_col,
"""

_SCHEMA_YML = """
version: 2
models:
  - name: model
    tests:
      - is_type:
          column_map:
            int64_col: ['integer', 'number']
            float64_col: ['float', 'number']
            numeric_col: ['numeric', 'number']
            string_col: ['string', 'not number']
"""

class TestBigQueryColumnTypes(BaseColumnTypes):

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "model.sql": _MODEL_SQL,
            "schema.yml": _SCHEMA_YML
        }

    def test_run_and_test(self, project):
        self.run_and_test()