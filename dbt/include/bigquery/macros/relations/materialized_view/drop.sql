{% macro bigquery__drop_materialized_view_sql(relation) %}
    drop materialized view if exists {{ relation }}
{% endmacro %}
