{% macro bigquery__refresh_materialized_view(relation) %}
    get_replace_materialized_view_as_sql(
        relation,
        sql,
        existing_relation,
        intermediate_relation,
        backup_relation,
        temp_relation
    )
{% endmacro %}
