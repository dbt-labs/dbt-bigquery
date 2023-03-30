{% macro bigquery__create_materialized_view_as(relation, sql) %}
    {% set proxy_view = bigquery__create_view_as(relation, sql) %}
    {{ return(proxy_view) }}
{% endmacro %}
