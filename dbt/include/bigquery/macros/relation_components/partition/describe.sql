{% macro bigquery__get_describe_partition_sql(relation) %}
    with max_partition_id as (
        select
            table_name,
            table_schema,
            table_catalog,
            max(partition_id) as partition_id
        from {{ relation.information_schema('PARTITIONS') }}
        where table_name = '{{ relation.identifier }}'
        and table_schema = '{{ relation.schema }}'
        and table_catalog = '{{ relation.database }}'
        group by
            table_name,
            table_schema,
            table_catalog
    )

    select
        c.column_name as partition_column_name,
        c.data_type as partition_data_type,
        case
            when regexp_contains(p.partition_id, '^[0-9]{4}$') THEN 'year'
            when regexp_contains(p.partition_id, '^[0-9]{6}$') THEN 'month'
            when regexp_contains(p.partition_id, '^[0-9]{8}$') THEN 'day'
            when regexp_contains(p.partition_id, '^[0-9]{10}$') THEN 'hour'
        end as partition_type
    from {{ relation.information_schema('COLUMNS') }} c
    left join max_partition_id p
        on p.table_name = c.table_name
        and p.table_schema = c.table_schema
        and p.table_catalog = c.table_catalog
    where c.table_name = '{{ relation.identifier }}'
    and c.table_schema = '{{ relation.schema }}'
    and c.table_catalog = '{{ relation.database }}'
    and c.is_partitioning_column = 'YES'
{% endmacro %}


{% macro bigquery__describe_partition(relation) %}
    {% set _sql = bigquery__get_describe_partition_sql(relation) %}
    {% do return(run_query(_sql)) %}
{% endmacro %}
