{% macro bigquery__format_column(column) -%}
  {% set data_type = column.data_type %}
  {% set formatted = column.column.lower() ~ " " ~ data_type %}
  {{ return({'name': column.name, 'data_type': data_type, 'formatted': formatted}) }}
{%- endmacro -%}

{% macro bigquery__get_empty_schema_sql(columns) %}
    {%- set columns = adapter.nest_columns(columns) -%}
    {{ return(dbt.default__get_empty_schema_sql(columns)) }}
{% endmacro %}

{% macro bigquery__get_select_subquery(sql) %}
    {%- set columns = adapter.nest_columns(model['columns']) -%}
    select
    {% for column in columns %}
      {{ column }}{{ ", " if not loop.last }}
    {% endfor %}
    from (
        {{ sql }}
    ) as model_subq
{%- endmacro %}

{% macro bigquery__get_table_columns_and_constraints() -%}
  {{ return(bigquery_table_columns_and_constraints()) }}
{%- endmacro %}

{% macro bigquery_table_columns_and_constraints() %}
  {# loop through user_provided_columns to create DDL with data types and constraints #}
    {%- set raw_column_constraints = adapter.render_raw_columns_constraints(raw_columns=model['columns']) -%}
    {%- set raw_model_constraints = adapter.render_raw_model_constraints(raw_constraints=model['constraints']) -%}
    (
    {% for c in raw_column_constraints -%}
      {{ c }}{{ "," if not loop.last or raw_model_constraints }}
    {% endfor %}
    {% for c in raw_model_constraints -%}
        {{ c }}{{ "," if not loop.last }}
    {% endfor -%}
    )
{% endmacro %}
