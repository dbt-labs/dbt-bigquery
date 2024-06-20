{% macro bigquery__get_replace_materialized_view_as_sql(relation, sql) %}

    {%- set materialized_view = adapter.Relation.materialized_view_from_relation_config(config.model) -%}

    create or replace materialized view {{ relation }}
        {% if materialized_view.partition -%}
            {{ partition_by(materialized_view.partition) }}
        {% endif -%}
        {%- if materialized_view.cluster -%}
            {{ cluster_by(materialized_view.cluster.fields) }}
        {%- endif %}
        options(
            {% if materialized_view.enable_refresh -%}
                enable_refresh = {{ materialized_view.enable_refresh }}
            {% endif -%}
            {%- if materialized_view.refresh_interval_minutes -%}
                ,refresh_interval_minutes = {{ materialized_view.refresh_interval_minutes }}
            {% endif -%}
            {%- if materialized_view.expiration_timestamp -%}
                ,expiration_timestamp = '{{ materialized_view.expiration_timestamp }}'
            {% endif -%}
            {%- if materialized_view.max_staleness -%}
                ,max_staleness = {{ materialized_view.max_staleness }}
            {% endif -%}
            {%- if materialized_view.allow_non_incremental_definition -%}
                ,allow_non_incremental_definition = {{ materialized_view.allow_non_incremental_definition }}
            {% endif -%}
            {%- if materialized_view.kms_key_name -%}
                ,kms_key_name = '{{ materialized_view.kms_key_name }}'
            {% endif -%}
            {%- if materialized_view.description -%}
                ,description = ""{{ materialized_view.description|tojson|safe }}""
            {% endif -%}
            {%- if materialized_view.labels -%}
                ,labels = {{ materialized_view.labels.items()|list }}
            {%- endif %}
        )
    as {{ sql }}

{% endmacro %}
