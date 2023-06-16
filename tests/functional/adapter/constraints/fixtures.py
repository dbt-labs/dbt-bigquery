my_model_struct_wrong_data_type_sql = """
{{ config(materialized = "table") }}

select
  STRUCT(1 AS struct_column_being_tested, "test" AS another_struct_column) as a
"""

my_model_struct_correct_data_type_sql = """
{{ config(materialized = "table")}}

select
  STRUCT("test" AS struct_column_being_tested, "test" AS b) as a
"""

model_struct_data_type_schema_yml = """
version: 2
models:
  - name: contract_struct_wrong
    config:
      contract:
        enforced: true
    columns:
      - name: a.struct_column_being_tested
        data_type: string
      - name: a.b
        data_type: string

  - name: contract_struct_correct
    config:
      contract:
        enforced: true
    columns:
      - name: a.struct_column_being_tested
        data_type: string
      - name: a.b
        data_type: string
"""

my_model_double_struct_wrong_data_type_sql = """
{{ config(materialized = "table") }}

select
  STRUCT(
    STRUCT(1 AS struct_column_being_tested, "test" AS c) as b,
    "test" as d
    ) as a
"""

my_model_double_struct_correct_data_type_sql = """
{{ config(materialized = "table") }}

select
  STRUCT(
    STRUCT("test" AS struct_column_being_tested, "test" AS c) as b,
    "test" as d
    ) as a
"""

model_double_struct_data_type_schema_yml = """
version: 2
models:
  - name: contract_struct_wrong
    config:
      contract:
        enforced: true
    columns:
      - name: a.b.struct_column_being_tested
        data_type: string
      - name: a.b.c
        data_type: string
      - name: a.d
        data_type: string

  - name: contract_struct_correct
    config:
      contract:
        enforced: true
    columns:
      - name: a.b.struct_column_being_tested
        data_type: string
      - name: a.b.c
        data_type: string
      - name: a.d
        data_type: string
"""


my_model_struct_sql = """
{{
  config(
    materialized = "table"
  )
}}

select STRUCT("test" as nested_column, "test" as nested_column2) as id
"""


model_struct_schema_yml = """
version: 2
models:
  - name: my_model
    config:
      contract:
        enforced: true
    columns:
      - name: id.nested_column
        quote: true
        data_type: string
        description: hello
        constraints:
          - type: not_null
          - type: unique
      - name: id.nested_column2
        data_type: string
        constraints:
          - type: unique
"""
