_MACRO_TEST_ALTER_COLUMN_TYPE = """
{% macro test_alter_column_type(model_name, column_name, new_column_type) %}
  {% set relation = ref(model_name) %}
  {{ alter_column_type(relation, column_name, new_column_type) }}
{% endmacro %}
"""

_MODEL_SQL = """
select
    CAST(1 as int64) as int64_col,
    CAST(2.0 as float64) as float64_col,
    CAST(3.0 as numeric) as numeric_col,
    CAST('3' as string) as string_col,
"""

_MODEL_ALT_SQL = """
{{ config(materialized='table') }}
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

_ALT_SCHEMA_YML = """
version: 2
models:
  - name: model
    tests:
      - is_type:
          column_map:
            int64_col: ['string', 'not number']
            float64_col: ['float', 'number']
            numeric_col: ['numeric', 'number']
            string_col: ['string', 'not number']
"""
