{% macro bigquery__refresh_materialized_view(relation) %}
    CALL BQ.REFRESH_MATERIALIZED_VIEW('{{ relation.database }}.{{ relation.schema }}.{{ relation.identifier }}');
{% endmacro %}
