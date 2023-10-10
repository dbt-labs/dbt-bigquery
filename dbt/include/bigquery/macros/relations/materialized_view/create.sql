{% macro bigquery__get_create_materialized_view_as_sql(relation, sql) %}

    {%- set materialized_view = adapter.Relation.materialized_view_from_model_node(config.model) -%}

    create materialized view if not exists {{ relation }}
    {% if materialized_view.partition %}{{ partition_by(materialized_view.partition) }}{% endif %}
    {% if materialized_view.cluster %}{{ cluster_by(materialized_view.cluster.fields) }}{% endif %}
    {{ bigquery_options(materialized_view.options.as_ddl_dict()) }}
    as {{ sql }}

{% endmacro %}
