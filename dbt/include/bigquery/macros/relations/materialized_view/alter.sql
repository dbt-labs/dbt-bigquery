{% macro bigquery__get_alter_materialized_view_as_sql(
    relation,
    configuration_changes,
    sql,
    existing_relation,
    backup_relation,
    intermediate_relation
) %}

    {% if configuration_changes.requires_full_refresh %}
        {{ get_replace_sql(existing_relation, relation, sql) }}
    {% else %}

        {%- set _needs_comma = False -%}

        alter materialized view {{ relation }}
        set options(
            {% if configuration_changes.enable_refresh -%}
                {%- if _needs_comma -%},{%- endif -%}
                {%- if configuration_changes.enable_refresh.action == 'drop' -%}
                    enable_refresh = NULL
                {%- else -%}
                    enable_refresh = {{ configuration_changes.enable_refresh.context }}
                {%- endif -%}
                {%- set _needs_comma = True -%}
            {% endif -%}
            {%- if configuration_changes.refresh_interval_minutes -%}
                {%- if _needs_comma -%},{%- endif -%}
                {%- if configuration_changes.refresh_interval_minutes.action == 'drop' -%}
                    refresh_interval_minutes = NULL
                {%- else -%}
                    refresh_interval_minutes = {{ configuration_changes.refresh_interval_minutes.context }}
                {%- endif -%}
                {%- set _needs_comma = True -%}
            {% endif -%}
            {%- if configuration_changes.max_staleness -%}
                {%- if _needs_comma -%},{%- endif -%}
                {%- if configuration_changes.max_staleness.action == 'drop' -%}
                    max_staleness = NULL
                {%- else -%}
                    max_staleness = {{ configuration_changes.max_staleness.context }}
                {%- endif -%}
                {%- set _needs_comma = True -%}
            {% endif -%}
            {%- if configuration_changes.kms_key_name -%}
                {%- if _needs_comma -%},{%- endif -%}
                {%- if configuration_changes.kms_key_name.action == 'drop' -%}
                    kms_key_name = NULL
                {%- else -%}
                    kms_key_name = '{{ configuration_changes.kms_key_name.context }}'
                {%- endif -%}
                {%- set _needs_comma = True -%}
            {% endif -%}
            {%- if configuration_changes.description -%}
                {%- if _needs_comma -%},{%- endif -%}
                {%- if configuration_changes.description.action == 'drop' -%}
                    description = NULL
                {%- else -%}
                    description = ""{{ configuration_changes.description.context|tojson|safe }}""
                {%- endif -%}
                {%- set _needs_comma = True -%}
            {% endif -%}
            {%- if configuration_changes.labels -%}
                {%- if _needs_comma -%},{%- endif -%}
                {%- if configuration_changes.labels.action == 'drop' -%}
                    labels = NULL
                {%- else -%}
                    labels = {{ configuration_changes.labels.context.items()|list }}
                {%- endif -%}
                {%- set _needs_comma = True -%}
            {%- endif -%}
        )

    {%- endif %}

{% endmacro %}

{% macro bigquery__get_materialized_view_configuration_changes(existing_relation, new_config) %}
    {% set _existing_materialized_view = adapter.describe_relation(existing_relation) %}
    {% set _configuration_changes = existing_relation.materialized_view_config_changeset(_existing_materialized_view, new_config.model) %}
    {% do return(_configuration_changes) %}
{% endmacro %}
