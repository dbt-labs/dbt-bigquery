{% macro bigquery__get_create_materialized_view_as_sql(relation, sql) %}
    create materialized view if not exists {{ relation }} as {{ sql }}
{% endmacro %}
