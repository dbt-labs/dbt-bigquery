{% macro bq_generate_microbatch_build_sql(
      tmp_relation, target_relation, sql, unique_key, partition_by, partitions, dest_columns, tmp_relation_exists, copy_partitions
) %}
    {% if partition_by is none %}
      {% set missing_partition_msg -%}
      The 'microbatch' strategy requires the `partition_by` config.
      {%- endset %}
      {% do exceptions.raise_compiler_error(missing_partition_msg) %}
    {% endif %}

    {% if partition_by.granularity != config.get('batch_size') %}
      {% set invalid_partition_by_granularity_msg -%}
      The 'microbatch' strategy in requires the `partition_by` config with the same granularity
      as its configured `batch_size`. Got:
        `batch_size`: {{ config.get('batch_size') }}
        `partition_by.granularity`: {{ partition_by.granularity }}
      {%- endset %}
      {% do exceptions.raise_compiler_error(invalid_partition_by_granularity_msg) %}
    {% endif %}

    {% set build_sql = bq_insert_overwrite_sql(
        tmp_relation, target_relation, sql, unique_key, partition_by, partitions, dest_columns, tmp_relation_exists, copy_partitions
    ) %}

    {{ return(build_sql) }}

{% endmacro %}
