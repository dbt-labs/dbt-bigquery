{% macro bigquery__get_describe_options_sql(relation) %}
    select
        option_name,
        option_value
    from {{ relation.information_schema('TABLE_OPTIONS') }}
    where table_name = '{{ relation.identifier }}'
    and table_schema = '{{ relation.schema }}'
    and table_catalog = '{{ relation.database }}'
{% endmacro %}


{% macro bigquery__describe_options(relation) %}
    {%- set _sql = bigquery__get_describe_options_sql(relation) -%}
    {% do return(run_query(_sql)) %}
{% endmacro %}
