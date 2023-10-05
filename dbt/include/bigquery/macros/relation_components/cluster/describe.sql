{% macro bigquery__get_describe_cluster_sql(relation) %}
    select
        column_name
    from {{ relation.information_schema('COLUMNS') }}
    where table_name = '{{ relation.identifier }}'
    and table_schema = '{{ relation.schema }}'
    and table_catalog = '{{ relation.database }}'
    and clustering_ordinal_position is not null
{% endmacro %}


{% macro bigquery__describe_cluster(relation) %}
    {%- set _sql = bigquery__get_describe_cluster_sql(relation) -%}
    {% do return(run_query(_sql)) %}
{% endmacro %}
