{% macro bigquery__get_replace_materialized_view_as_sql(relation, sql) %}

    {%- set materialized_view = adapter.Relation.materialized_view_from_relation_config(config.model) -%}

    {%- set _needs_comma = False -%}

    create or replace materialized view {{ relation }}
        {% if materialized_view.partition -%}
            {{ partition_by(materialized_view.partition) }}
        {% endif -%}
        {%- if materialized_view.cluster -%}
            {{ cluster_by(materialized_view.cluster.fields) }}
        {%- endif %}
        options(
            {% if materialized_view.enable_refresh -%}
                {%- if _needs_comma -%},{%- endif -%}
                enable_refresh = {{ materialized_view.enable_refresh }}
                {%- set _needs_comma = True -%}
            {% endif -%}
            {%- if materialized_view.refresh_interval_minutes -%}
                {%- if _needs_comma -%},{%- endif -%}
                refresh_interval_minutes = {{ materialized_view.refresh_interval_minutes }}
                {%- set _needs_comma = True -%}
            {% endif -%}
            {%- if materialized_view.expiration_timestamp -%}
                {%- if _needs_comma -%},{%- endif -%}
                expiration_timestamp = '{{ materialized_view.expiration_timestamp }}'
                {%- set _needs_comma = True -%}
            {% endif -%}
            {%- if materialized_view.max_staleness -%}
                {%- if _needs_comma -%},{%- endif -%}
                max_staleness = {{ materialized_view.max_staleness }}
                {%- set _needs_comma = True -%}
            {% endif -%}
            {%- if materialized_view.allow_non_incremental_definition -%}
                {%- if _needs_comma -%},{%- endif -%}
                allow_non_incremental_definition = {{ materialized_view.allow_non_incremental_definition }}
                {%- set _needs_comma = True -%}
            {% endif -%}
            {%- if materialized_view.kms_key_name -%}
                {%- if _needs_comma -%},{%- endif -%}
                kms_key_name = '{{ materialized_view.kms_key_name }}'
                {%- set _needs_comma = True -%}
            {% endif -%}
            {%- if materialized_view.description -%}
                {%- if _needs_comma -%},{%- endif -%}
                description = ""{{ materialized_view.description|tojson|safe }}""
                {%- set _needs_comma = True -%}
            {% endif -%}
            {%- if materialized_view.labels -%}
                {%- if _needs_comma -%},{%- endif -%}
                labels = {{ materialized_view.labels.items()|list }}
                {%- set _needs_comma = True -%}
            {%- endif %}
        )
    as {{ sql }}

{% endmacro %}
