{% macro bq_generate_incremental_merge_build_sql(
    tmp_relation, target_relation, sql, unique_key, partition_by, dest_columns, tmp_relation_exists, incremental_predicates
) %}
    {%- set source_sql -%}
        {%- if tmp_relation_exists -%}
        (
        select
        {% if partition_by.time_ingestion_partitioning -%}
        {{ partition_by.insertable_time_partitioning_field() }},
        {%- endif -%}
        * from {{ tmp_relation }}
        )
        {%- else -%} {#-- wrap sql in parens to make it a subquery --#}
        (
            {%- if partition_by.time_ingestion_partitioning -%}
            {{ wrap_with_time_ingestion_partitioning_sql(partition_by, sql, True) }}
            {%- else -%}
            {{sql}}
            {%- endif %}
        )
        {%- endif -%}
    {%- endset -%}

    {% set build_sql = get_merge_sql(target_relation, source_sql, unique_key, dest_columns, incremental_predicates) %}

    {{ return(build_sql) }}

{% endmacro %}
