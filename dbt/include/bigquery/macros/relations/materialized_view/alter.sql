{% macro bigquery__get_alter_materialized_view_as_sql(
    relation,
    configuration_changes,
    sql,
    existing_relation
) %}

    {% if configuration_changes.requires_full_refresh %}
        {{ get_replace_sql(existing_relation, relation, sql) }}
    {% else %}

        {%- set auto_refresh = configuration_changes.auto_refresh -%}
        {%- if auto_refresh -%}{{- log('Applying UPDATE AUTOREFRESH to: ' ~ relation) -}}{%- endif -%}

        alter materialized view {{ relation }}
            set options (
                {% if auto_refresh %} enable_refresh = {{ auto_refresh.context }}{% endif %}
            )

    {%- endif %}


{% endmacro %}

{% macro bigquery__get_materialized_view_configuration_changes(existing_relation, new_config) %}
    {% set _existing_materialized_view = bigquery__describe_materialized_view(existing_relation) %}
    {% set _configuration_changes = existing_relation.materialized_view_config_changeset(_existing_materialized_view, new_config) %}
    {% do return(_configuration_changes) %}
{% endmacro %}
