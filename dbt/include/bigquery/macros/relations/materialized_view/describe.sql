{% macro bigquery__describe_materialized_view(relation) %}
    {%- set _materialized_view_sql -%}
        select
            mv.table_name as materialized_view,
            c.column_name,
            c.is_partitioning_column,
            c.clustering_ordinal_position,
            topt.option_name,
            topt.option_value,
            topt.option_type
        from
            `{{ relation.database }}.{{ relation.schema }}.INFORMATION_SCHEMA.MATERIALIZED_VIEWS` mv
        left join
            `{{ relation.database }}.{{ relation.schema }}.INFORMATION_SCHEMA.COLUMNS` c
        on
            mv.table_name = c.table_name
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
