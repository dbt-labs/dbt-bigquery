{% macro bigquery__get_describe_partition_sql(relation) %}
    select
        c.column_name as partition_column_name,
        c.data_type as partition_data_type,
        case
            when regexp_contains(p.partition_id, '^[0-9]{4}$') THEN 'year'
            when regexp_contains(p.partition_id, '^[0-9]{6}$') THEN 'month'
            when regexp_contains(p.partition_id, '^[0-9]{8}$') THEN 'day'
            when regexp_contains(p.partition_id, '^[0-9]{10}$') THEN 'hour'
        end as partition_type
    from {{ relation.information_schema('PARTITIONS') }} p
    join {{ relation.information_schema('COLUMNS') }} c
        on c.table_name = p.table_name
        and c.table_schema = p.table_schema
        and c.table_catalog = p.table_catalog
    where p.table_name = '{{ relation.identifier }}'
    and p.table_schema = '{{ relation.schema }}'
    and p.table_catalog = '{{ relation.database }}'
    and c.is_partitioning_column = 'YES'
{% endmacro %}


{% macro bigquery__describe_partition(relation) %}
    {% set _sql = bigquery__get_describe_partition_sql(relation) %}
    {% do return(run_query(_sql)) %}
{% endmacro %}
