{% macro bigquery__alter_materialized_view(relation, sql) %}
    {% set proxy_view = bigquery__create_view_as(relation, sql) %}
    {{ return(proxy_view) }}
{% endmacro %}
