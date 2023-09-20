{% macro bigquery__get_alter_materialized_view_as_sql(
    relation,
    configuration_changes,
    sql,
    existing_relation
) %}

    {% if configuration_changes.requires_full_refresh %}

        {{ bigquery__get_replace_materialized_view_as_sql(relation, sql) }}

    {% else %}


{% endmacro %}

{% macro bigquery__get_materialized_view_configuration_changes(existing_relation, new_config) %}
    {% set _existing_materialized_view = bigquery__describe_materialized_view(existing_relation) %}
    {% set _configuration_changes = existing_relation.materialized_view_config_changeset(_existing_materialized_view, new_config) %}
    {% do return(_configuration_changes) %}
{% endmacro %}
