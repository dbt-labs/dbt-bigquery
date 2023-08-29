{% macro bigquery__get_alter_materialized_view_as_sql(
    relation,
    configuration_changes,
    sql,
    existing_relation,
    intermediate_relation
) %}
    get_replace_materialized_view_as_sql(
        relation,
        sql,
        existing_relation,
        intermediate_relation,
        backup_relation,
        temp_relation
    )
{% endmacro %}
