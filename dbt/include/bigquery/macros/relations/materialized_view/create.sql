{% macro bigquery__get_create_materialized_view_as_sql(relation, sql) %}

    {%- set materialized_view = adapter.Relation.materialized_view_from_model_node(model) -%}

    create materialized view if not exists {{ relation }}
    {{ partition_by(materialized_view.partition) }}
    {{ cluster_by(materialized_view.cluster.fields) }}
    {{ bigquery_options(adapter.materialized_view_options(materialized_view)) }}
    as {{ sql }}

{% endmacro %}
