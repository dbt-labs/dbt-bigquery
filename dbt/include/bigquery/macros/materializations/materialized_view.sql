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

{% materialization materialized_view, adapter='bigquery' %}
    {% set existing_relation = load_cached_relation(this) %}
    {% set target_relation = this.incorporate(type=this.MaterializedView) %}
    {% set intermediate_relation = make_intermediate_relation(target_relation) %}


    {{ materialized_view_setup(backup_relation, intermediate_relation, pre_hooks) }}

        {% set build_sql = materialized_view_get_build_sql(existing_relation, target_relation, backup_relation, intermediate_relation) %}

        {% if build_sql == '' %}
            {{ materialized_view_execute_no_op(target_relation) }}
        {% else %}
            {{ materialized_view_execute_build_sql(build_sql, existing_relation, target_relation, post_hooks) }}
        {% endif %}

    {{ materialized_view_teardown(backup_relation, intermediate_relation, post_hooks) }}

    {{ return({'relations': [target_relation]}) }}

{% endmaterialization %}
