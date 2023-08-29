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
