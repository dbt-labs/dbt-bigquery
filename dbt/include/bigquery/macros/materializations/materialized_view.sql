

{% macro bigquery__get_create_materialized_view_as_sql(relation, sql) %}
    create materialized view if not exists {{ relation }} as {{ sql }}
{% endmacro %}

{% macro bigquery__drop_materialized_view_sql(relation) %}
    drop materialized view if exists {{ relation }}
{% endmacro %}

{% macro bigquery__get_replace_materialized_view_as_sql(
    relation,
    sql,
    existing_relation,
    backup_relation,
    intermediate_relation
) %}
     bigquery__drop_relation_sql(existing_relation)
     get_create_materialized_view_as_sql(intermediate_relation, sql)
{% endmacro %}

{% macro bigquery__refresh_materialized_view(relation) %}
    get_replace_materialized_view_as_sql(
        relation,
        sql,
        existing_relation,
        backup_relation,
        intermediate_relation
    )
{% endmacro %}

{% macro bigquery__get_alter_materialized_view_as_sql(
    relation,
    configuration_changes,
    sql,
    existing_relation,
    backup_relation,
    intermediate_relation
) %}
    get_replace_materialized_view_as_sql(
        relation,
        sql,
        existing_relation,
        backup_relation,
        intermediate_relation
    )
{% endmacro %}

{% macro bigquery__get_materialized_view_configuration_changes() %}
    return none
{% endmacro %}
