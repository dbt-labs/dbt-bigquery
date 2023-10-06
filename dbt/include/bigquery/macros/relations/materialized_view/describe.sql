{% macro bigquery__describe_materialized_view(relation) %}
    {%- set _materialized_view_sql -%}
        select
            table_name,
            table_schema,
            table_catalog
        from {{ relation.information_schema('MATERIALIZED_VIEWS') }}
        where table_name = '{{ relation.identifier }}'
        and table_schema = '{{ relation.schema }}'
        and table_catalog = '{{ relation.database }}'
    {%- endset %}
    {% set _materialized_view = run_query(_materialized_view_sql) %}

    {%- set _cluster_by = bigquery__describe_cluster(relation) -%}
    {%- set _options = bigquery__describe_options(relation) -%}

    {% do return({
        'materialized_view': _materialized_view,
        'cluster_by': _cluster_by,
        'options': _options
    }) %}
{% endmacro %}
