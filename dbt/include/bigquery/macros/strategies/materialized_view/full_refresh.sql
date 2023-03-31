{% macro bigquery__strategy__materialized_view__full_refresh(relation, sql) %}
    {{ drop_relation_if_exists(relation) }}
    {{ bigquery__db_api__materialized_view__create(relation, sql) }}
{% endmacro %}
