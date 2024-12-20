#
# Models
#

clustered_model_sql = """
{{
    config(
        materialized = "table",
        partition_by = {"field": "updated_at", "data_type": "date"},
        cluster_by = "dupe",
    )
}}

select * from {{ ref('view_model') }}
""".lstrip()

funky_case_sql = """
select 1 as id
""".lstrip()

labeled_model_sql = """
{{
    config(
        materialized = "table",
        labels = {'town': 'fish', 'analytics': 'yes'}
    )
}}

select * from {{ ref('view_model') }}
""".lstrip()

multi_clustered_model_sql = """
{{
    config(
        materialized = "table",
        partition_by = {"field": "updated_at", "data_type": "date"},
        cluster_by = ["dupe","id"],
    )
}}

select * from {{ ref('view_model') }}
""".lstrip()


partitioned_model_sql = """
{{
    config(
        materialized = "table",
        partition_by = {'field': 'updated_at', 'data_type': 'date'},
    )
}}

select * from {{ ref('view_model') }}
""".lstrip()

schema_yml = """
version: 2
models:
- name: view_model
  description: |
    View model description "with double quotes"
    and with 'single  quotes' as welll as other;
    '''abc123'''
    reserved -- characters
    --
    /* comment */
  columns:
  - name: dupe
    tests:
    - unique
  - name: id
    tests:
    - not_null
    - unique
  - name: updated_at
    tests:
    - not_null
  tests:
  - was_materialized:
      name: view_model
      type: view
- name: table_model
  description: |
    View model description "with double quotes"
    and with 'single  quotes' as welll as other;
    '''abc123'''
    reserved -- characters
    --
    /* comment */
  columns:
  - name: id
    tests:
    - not_null
  tests:
  - was_materialized:
      name: table_model
      type: table
- name: fUnKyCaSe
  columns:
    - name: id
      tests:
        - not_null
        - unique
  tests:
    - was_materialized:
        name: fUnKyCaSe
        type: view


sources:
  - name: raw
    project: "{{ target.database }}"
    dataset: "{{ target.schema }}"
    tables:
      - name: seed
        identifier: data_seed
""".lstrip()

sql_header_model_sql = """
{{ config(materialized="table") }}

{# This will fail if it is not extracted correctly #}
{% call set_sql_header(config) %}
    CREATE TEMPORARY FUNCTION a_to_b(str STRING)
    RETURNS STRING AS (
      CASE
      WHEN LOWER(str) = 'a' THEN 'b'
      ELSE str
      END
    );
{% endcall %}

select a_to_b(dupe) as dupe from {{ ref('view_model') }}
""".lstrip()

sql_header_model_incr_sql = """
{{ config(materialized="incremental") }}

{# This will fail if it is not extracted correctly #}
{% call set_sql_header(config) %}
    DECLARE int_var INT64 DEFAULT 42;

    CREATE TEMPORARY FUNCTION a_to_b(str STRING)
    RETURNS STRING AS (
      CASE
      WHEN LOWER(str) = 'a' THEN 'b'
      ELSE str
      END
    );
{% endcall %}

select a_to_b(dupe) as dupe from {{ ref('view_model') }}
""".lstrip()

sql_header_model_incr_insert_overwrite_sql = """
{#
    Ensure that the insert overwrite incremental strategy
    works correctly when a UDF is used in a sql_header. The
    failure mode here is that dbt might inject the UDF header
    twice: once for the `create table` and then again for the
    merge statement.
#}

{{ config(
    materialized="incremental",
    incremental_strategy='insert_overwrite',
    partition_by={"field": "dt", "data_type": "date"}
) }}

{# This will fail if it is not extracted correctly #}
{% call set_sql_header(config) %}
    DECLARE int_var INT64 DEFAULT 42;

    CREATE TEMPORARY FUNCTION a_to_b(str STRING)
    RETURNS STRING AS (
      CASE
      WHEN LOWER(str) = 'a' THEN 'b'
      ELSE str
      END
    );
{% endcall %}

select
    current_date() as dt,
    a_to_b(dupe) as dupe

from {{ ref('view_model') }}
""".lstrip()

sql_header_model_incr_insert_overwrite_static_sql = """
{#
    Ensure that the insert overwrite incremental strategy
    works correctly when a UDF is used in a sql_header. The
    failure mode here is that dbt might inject the UDF header
    twice: once for the `create table` and then again for the
    merge statement.
#}

{{ config(
    materialized="incremental",
    incremental_strategy='insert_overwrite',
    partition_by={"field": "dt", "data_type": "date"},
    partitions=["'2020-01-1'"]
) }}

{# This will fail if it is not extracted correctly #}
{% call set_sql_header(config) %}
    CREATE TEMPORARY FUNCTION a_to_b(str STRING)
    RETURNS STRING AS (
      CASE
      WHEN LOWER(str) = 'a' THEN 'b'
      ELSE str
      END
    );
{% endcall %}

select
    cast('2020-01-01' as date) as dt,
    a_to_b(dupe) as dupe

from {{ ref('view_model') }}
""".lstrip()

tabel_model_sql = """
{{
  config(
    materialized = "table",
    persist_docs={ "relation": true, "columns": true, "schema": true }
  )
}}

select * from {{ ref('view_model') }}
""".lstrip()

view_model_sql = """
{{
  config(
    materialized = "view",
    persist_docs={ "relation": true, "columns": true, "schema": true }
  )
}}


select
    id,
    current_date as updated_at,
    dupe

from {{ source('raw', 'seed') }}
""".lstrip()

#
# Macros
#

test_creation_sql = """
{% test was_materialized(model, name, type) %}

    {#-- don't run this query in the parsing step #}
    {%- if model -%}
        {%- set table = adapter.get_relation(database=model.database, schema=model.schema,
                                             identifier=model.name) -%}
    {%- else -%}
        {%- set table = {} -%}
    {%- endif -%}

    {% if table %}
      select '{{ table.type }} does not match expected value {{ type }}'
      from (select true)
      where '{{ table.type }}' != '{{ type }}'
    {% endif %}

{% endtest %}
""".lstrip()

test_int_inference_sql = """
{% macro assert_eq(value, expected, msg) %}
    {% if value != expected %}
        {% do exceptions.raise_compiler_error(msg ~ value) %}
    {% endif %}
{% endmacro %}


{% macro test_int_inference() %}

    {% set sql %}
        select
            0 as int_0,
            1 as int_1,
            2 as int_2
    {% endset %}

    {% set result = run_query(sql) %}
    {% do assert_eq((result | length), 1, 'expected 1 result, got ') %}
    {% set actual_0 = result[0]['int_0'] %}
    {% set actual_1 = result[0]['int_1'] %}
    {% set actual_2 = result[0]['int_2'] %}

    {% do assert_eq(actual_0, 0, 'expected expected actual_0 to be 0, it was ') %}
    {% do assert_eq((actual_0 | string), '0', 'expected string form of actual_0 to be 0, it was ') %}
    {% do assert_eq((actual_0 * 2), 0, 'expected actual_0 * 2 to be 0, it was ') %} {# not 00 #}

    {% do assert_eq(actual_1, 1, 'expected actual_1 to be 1, it was ') %}
    {% do assert_eq((actual_1 | string), '1', 'expected string form of actual_1 to be 1, it was ') %}
    {% do assert_eq((actual_1 * 2), 2, 'expected actual_1 * 2 to be 2, it was ') %} {# not 11 #}

    {% do assert_eq(actual_2, 2, 'expected actual_2 to be 2, it was ') %}
    {% do assert_eq((actual_2 | string), '2', 'expected string form of actual_2 to be 2, it was ') %}
    {% do assert_eq((actual_2 * 2), 4, 'expected actual_2 * 2 to be 4, it was ') %}  {# not 22 #}

{% endmacro %}
""".lstrip()

test_project_for_job_id_sql = """
{% test project_for_job_id(model, region, unique_schema_id, project_id) %}
select 1
from `region-us`.INFORMATION_SCHEMA.JOBS_BY_PROJECT
where date(creation_time) = current_date
  and job_project = {{project_id}}
  and destination_table.dataset_id = {{unique_schema_id}}
{% endtest %}
""".lstrip()

wrapped_macros_sql = """
{% macro my_create_schema(db_name, schema_name) %}
    {% if not execute %}
        {% do return(None) %}
    {% endif %}
    {% set relation = api.Relation.create(database=db_name, schema=schema_name).without_identifier() %}
    {% do create_schema(relation) %}
{% endmacro %}

{% macro my_drop_schema(db_name, schema_name) %}
    {% if not execute %}
        {% do return(None) %}
    {% endif %}
    {% set relation = api.Relation.create(database=db_name, schema=schema_name).without_identifier() %}
    {% do drop_schema(relation) %}
{% endmacro %}


{% macro my_create_table_as(db_name, schema_name, table_name) %}
    {% if not execute %}
        {% do return(None) %}
    {% endif %}
    {% set relation = api.Relation.create(database=db_name, schema=schema_name, identifier=table_name) %}
    {% do run_query(create_table_as(false, relation, 'select 1 as id')) %}
{% endmacro %}


{% macro ensure_one_relation_in(db_name, schema_name) %}
    {% if not execute %}
        {% do return(None) %}
    {% endif %}
    {% set relation = api.Relation.create(database=db_name, schema=schema_name).without_identifier() %}
    {% set results = list_relations_without_caching(relation) %}
    {% set rlen = (results | length) %}
    {% if rlen != 1 %}
        {% do exceptions.raise_compiler_error('Incorect number of results (expected 1): ' ~ rlen) %}
    {% endif %}
    {% set result = results[0] %}
    {% set columns = get_columns_in_relation(result) %}
    {% set clen = (columns | length) %}
    {% if clen != 1 %}
        {% do exceptions.raise_compiler_error('Incorrect number of columns (expected 1): ' ~ clen) %}
    {% endif %}
{% endmacro %}
""".lstrip()
