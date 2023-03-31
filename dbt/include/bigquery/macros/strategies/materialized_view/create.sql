{% macro bigquery__strategy__materialized_view__create(relation, sql) %}
    {{ bigquery__db_api__materialized_view__create(relation, sql) }}
{% endmacro %}
