{% macro bigquery__refresh_materialized_view(relation, sql) %}
    {{ return({'relations': [relation]}) }}
{% endmacro %}
