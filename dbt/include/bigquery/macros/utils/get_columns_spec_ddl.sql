{% macro bigquery__format_column(column) -%}
  {% set data_type = column.data_type %}
  {% set formatted = column.column.lower() ~ " " ~ data_type %}
  {{ return({'name': column.name, 'data_type': data_type, 'formatted': formatted}) }}
{%- endmacro -%}

{% macro bigquery__get_empty_schema_sql(columns) %}
    {%- set columns = adapter.nest_column_data_types(columns) -%}
    {{ return(dbt.default__get_empty_schema_sql(columns)) }}
{% endmacro %}

{% macro bigquery__get_select_subquery(sql) %}
    {%- set columns = adapter.nest_column_data_types(model['columns']) -%}
    select
    {% for column in columns %}
      {{ column }}{{ ", " if not loop.last }}
    {% endfor %}
    from (
        {{ sql }}
    ) as model_subq
{%- endmacro %}
