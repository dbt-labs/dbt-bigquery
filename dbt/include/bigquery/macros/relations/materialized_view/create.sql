{% macro bigquery__get_create_materialized_view_as_sql(relation, sql) %}

    {%- set partition_config_raw = config.get('partition_by', none) -%}
    {%- set partition_config = adapter.parse_partition_by(partition_config_raw) -%}
    {%- if partition_config.time_ingestion_partitioning -%}
        {% do exceptions.raise_compiler_error("Time ingestion partitioning is not supported for materialized views") %}
    {%- endif -%}

    {%- set cluster_config = config.get('cluster_by', none) -%}

    create materialized view if not exists {{ relation }}
    {{ partition_by(partition_config) }}
    {{ cluster_by(cluster_config) }}
    {{ bigquery_options(adapter.materialized_view_options(config, model)) }}
    as {{ sql }}

{% endmacro %}
