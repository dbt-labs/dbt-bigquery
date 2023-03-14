{% macro bigquery__get_columns_spec_ddl() %}
  {# loop through user_provided_columns to create DDL with data types and constraints #}
    {%- set ns = namespace(at_least_one_check=False, at_least_one_pk=False) -%}
    {%- set user_provided_columns = model['columns'] -%}
    (
    {% for i in user_provided_columns %}
      {%- set col = user_provided_columns[i] -%}
      {%- set constraints = col['constraints'] -%}
      {{ col['name'] }} {{ col['data_type'] }}
      {%- for x in constraints -%}
        {%- if x.type == "check" -%}
          {%- set ns.at_least_one_check = True -%}
        {%- elif x.type == "primary_key" -%}
          {%- set ns.at_least_one_pk = True -%}
        {%- else -%}
          {{ " not null" if x.type == "not_null" else " unique" if x.type == "unique" else " primary key" if x.type == "primary_key" else " foreign key" if x.type == "foreign key" else "" }}{{ x.expression or "" }}
        {%- endif -%}
      {%- endfor -%}
      {{ "," if not loop.last }}
    {% endfor -%}
    )
  {%- if ns.at_least_one_check -%}
      {{exceptions.warn("We noticed you have check constraints in your configs. These are not compatible with BigQuery and will be ignored.")}}
  {%- endif -%}
  {%- if ns.at_least_one_pk -%}
    {{exceptions.warn("We noticed you have primary key constraints in your configs. These are not compatible with BigQuery and will be ignored.")}}
  {%- endif -%}
{% endmacro %}

{% macro bigquery__format_column(column) -%}
  {{ return(column.column.lower() ~ " " ~ column.data_type) }}
{%- endmacro -%}
