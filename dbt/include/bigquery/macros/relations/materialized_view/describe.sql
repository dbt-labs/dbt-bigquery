{% macro bigquery__describe_materialized_view(relation) %}
    {%- set _materialized_view_sql -%}
        select
            mv.table_name as materialized_view,
            pt.table_name as partitioned_table,
            pt.partitioning_type,
            pt.partitioning_field_name,
            topt.partition_expiration_days,
            topt.table_name as table_options_table,
            topt.description,
            topt.enable_refresh,
            topt.friendly_name,
            topt.expiration_timestamp as hours_to_expiration,
            topt.kms_key_name,
            topt.labels,
            topt.max_staleness,
            topt.refresh_interval_minutes,
        from
            `{{ relation.database }}.{{ relation.schema }}.INFORMATION_SCHEMA.MATERIALIZED_VIEWS` mv
        left join
            `{{ relation.database }}.{{ relation.schema }}.INFORMATION_SCHEMA.PARTITIONS` pt
        on
            mv.table_name = pt.table_name
        left join
            `{{ relation.database }}.{{ relation.schema }}.INFORMATION_SCHEMA.TABLE_OPTIONS` topt
        on
            mv.table_name = topt.table_name
        where
            mv.table_name = '{{ relation.name }}'
    {%- endset %}
    {% set _materialized_view = run_query(_materialized_view_sql) %}

    {% do return({'materialized_view': _materialized_viewy}) %}
{% endmacro %}
