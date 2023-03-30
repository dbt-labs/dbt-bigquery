{% macro bigquery__create_materialized_view(relation, sql) %}
    {% set proxy_view = bigquery__create_view_as(relation, sql) %}
    {{ return(proxy_view) }}
{% endmacro %}


{% macro bigquery__alter_materialized_view(relation, sql) %}
    {% set proxy_view = bigquery__create_view_as(relation, sql) %}
    {{ return(proxy_view) }}
{% endmacro %}


{% macro bigquery__refresh_materialized_view(relation, sql) %}
    {{ return({'relations': [relation]}) }}
{% endmacro %}
