{% macro materialized_view_setup(backup_relation, intermediate_relation, pre_hooks) %}
    {{ run_hooks(pre_hooks, inside_transaction=False) }}
{% endmacro %}


{% macro materialized_view_teardown(backup_relation, intermediate_relation, post_hooks) %}
    {{ run_hooks(post_hooks, inside_transaction=False) }}
{% endmacro %}
