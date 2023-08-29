{% macro bigquery__get_replace_materialized_view_as_sql(
    relation,
    sql,
    existing_relation,
    intermediate_relation,
    backup_relation,
    temp_relation
) %}
     {% do bigquery__drop_relation_sql(existing_relation) %}
    {{ get_create_materialized_view_as_sql(relation, sql) }}
{% endmacro %}
