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
    select {{ adapter.dispatch('get_column_names')() }}
    from (
        {{ sql }}
    ) as model_subq
{%- endmacro %}

{% macro bigquery__get_column_names() %}
  {#- loop through nested user_provided_columns to get column names -#}
    {%- set user_provided_columns = adapter.nest_column_data_types(model['columns']) -%}
    {%- for i in user_provided_columns %}
      {%- set col = user_provided_columns[i] -%}
      {%- set col_name = adapter.quote(col['name']) if col.get('quote') else col['name'] -%}
      {{ col_name }}{{ ", " if not loop.last }}
    {%- endfor -%}
{% endmacro %}
