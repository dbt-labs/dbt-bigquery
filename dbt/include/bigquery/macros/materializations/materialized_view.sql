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
    intermediate_relation,
    backup_relation,
    temp_relation
) %}
     {% do bigquery__drop_relation_sql(existing_relation) %}
    {{ get_create_materialized_view_as_sql(relation, sql) }}
{% endmacro %}

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

{% macro bigquery__get_materialized_view_configuration_changes() %}
    return none
{% endmacro %}

{% materialized_view_get_build_sql(existing_relation, target_relation, backup_relation, intermediate_relation) %}

    {% set full_refresh_mode = (should_full_refresh()) %}

    {% if existing_relation is none %}
        set build_sql = get_create_materialized_view_as_sql(target_relation, sql)
    {% elif full_refresh_mode or not existing_relation.is_materialized_view %}
        {% set build_sql = get_replace_materialized_view_as_sql(target_relation, sql, existing_relation, backup_relation, intermediate_relation, temp_relation) %}
    {% else %}
        {% set build_sql = refresh_materialized_view(relation) %}
    {% endif %}

    {% do return build_sql %}

{% endmacro %}



{% materialization materialized_view, adapter='bigquery' %}


    {% set existing_relation = load_relation(this) %}
    {% set target_relation = this.incorporate(type=this.MaterializedView) %}
    {% set temp_relation = make_temp_relation(target_relation) %}


    {{ run_hooks(pre_hooks, inside_transaction=False) }}

        {% set build_sql = materialized_view_get_build_sql(existing_relation, target_relation, temp_relation) %}

        {% if build_sql == '' %}
            {{ materialized_view_execute_no_op(target_relation) }}
        {% else %}
            {{ materialized_view_execute_build_sql(build_sql, existing_relation, target_relation, post_hooks) }}
        {% endif %}

    {{ run_hooks(post_hooks, inside_transaction=False) }}

    {{ return({'relations': [target_relation]}) }}

{% endmaterialization %}
