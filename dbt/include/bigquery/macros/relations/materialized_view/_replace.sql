{% macro bigquery__get_replace_materialized_view_as_sql(
    relation,
    sql
) %}
    {{ get_drop_sql(existing_relation) }}
    {{ get_create_materialized_view_as_sql(relation, sql) }}
{% endmacro %}
