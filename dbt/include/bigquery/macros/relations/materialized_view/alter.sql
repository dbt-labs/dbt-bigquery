{% macro bigquery__get_alter_materialized_view_as_sql(
    relation,
    configuration_changes,
    sql,
    existing_relation
) %}
    bigquery__get_replace_materialized_view_as_sql(
        relation,
        sql
    )
{% endmacro %}

{% macro bigquery__get_materialized_view_configuration_changes() %}
    {{- return(None) -}}
{% endmacro %}
