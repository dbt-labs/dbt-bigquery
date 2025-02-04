{% macro bigquery__get_replace_materialized_view_as_sql(relation, sql) %}

    {%- set materialized_view = adapter.Relation.materialized_view_from_relation_config(config.model) -%}

    {% if config.get('grant_access_to') %}
      {% for grant_target_dict in config.get('grant_access_to') %}
        {% do adapter.grant_access_to(this, 'view', None, grant_target_dict) %}
      {% endfor %}
    {% endif %}

    create or replace materialized view if not exists {{ relation }}
    {% if materialized_view.partition %}{{ partition_by(materialized_view.partition) }}{% endif %}
    {% if materialized_view.cluster %}{{ cluster_by(materialized_view.cluster.fields) }}{% endif %}
    {{ bigquery_options(materialized_view.options.as_ddl_dict()) }}
    as {{ sql }}

{% endmacro %}
