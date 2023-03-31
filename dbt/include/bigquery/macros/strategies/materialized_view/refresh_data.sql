{% macro bigquery__strategy__materialized_view__refresh_data(relation) %}
    {{ bigquery__db_api__materialized_view__refresh(relation) }}
{% endmacro %}
