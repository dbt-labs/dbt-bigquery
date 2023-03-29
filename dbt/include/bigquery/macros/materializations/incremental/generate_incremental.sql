{% macro bq_generate_incremental_build_sql(
    strategy,
    tmp_relation,
    target_relation,
    sql,
    unique_key,
    partition_by,
    partitions,
    dest_columns,
    tmp_relation_exists,
    copy_partitions,
    incremental_predicates
) %}
    {#-- if partitioned, use BQ scripting to get the range of partition values to be updated --#}
    {% if strategy == 'insert_overwrite' %}

        {% set build_sql = bq_generate_incremental_insert_overwrite_build_sql(
            tmp_relation,
            target_relation,
            sql,
            unique_key,
            partition_by,
            partitions,
            dest_columns,
            tmp_relation_exists,
            copy_partitions
        ) %}

    {% else %} {# strategy == 'merge' #}

        {% set build_sql = bq_generate_incremental_merge_build_sql(
            tmp_relation,
            target_relation,
            sql,
            unique_key,
            partition_by,
            dest_columns,
            tmp_relation_exists,
            incremental_predicates
        ) %}

    {% endif %}

    {{ return(build_sql) }}

{% endmacro %}


{% macro dbt_bigquery_validate_get_incremental_strategy(config) %}
    {#-- Find and validate the incremental strategy #}
    {% set strategy = config.get("incremental_strategy") or 'merge' %}

    {% set invalid_strategy_msg %}
        Invalid incremental strategy provided: {{ strategy }}
        Expected one of: 'merge', 'insert_overwrite'
    {% endset %}
    {% if strategy not in ['merge', 'insert_overwrite'] %}
        {% do exceptions.raise_compiler_error(invalid_strategy_msg) %}
    {% endif %}

    {% do return(strategy) %}

{% endmacro %}
