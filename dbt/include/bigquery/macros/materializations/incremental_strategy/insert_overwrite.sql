{% macro bq_generate_incremental_insert_overwrite_build_sql(
    tmp_relation, target_relation, sql, unique_key, partition_by, partitions, dest_columns, on_schema_change
) %}
    {% if partition_by is none %}
      {% set missing_partition_msg -%}
      The 'insert_overwrite' strategy requires the `partition_by` config.
      {%- endset %}
      {% do exceptions.raise_compiler_error(missing_partition_msg) %}
    {% endif %}

    {% set build_sql = bq_insert_overwrite(
        tmp_relation, target_relation, sql, unique_key, partition_by, partitions, dest_columns, on_schema_change
    ) %}

    {{ return(build_sql) }}

{% endmacro %}

{% macro bq_insert_overwrite(
    tmp_relation, target_relation, sql, unique_key, partition_by, partitions, dest_columns, tmp_relation_exists
) %}

  {% if partitions is not none and partitions != [] %} {# static #}

      {% set predicate -%}
          {{ partition_by.render_wrapped(alias='DBT_INTERNAL_DEST') }} in (
              {{ partitions | join (', ') }}
          )
      {%- endset %}

      {%- set source_sql -%}
        (
          {%- if partition_by.time_ingestion_partitioning -%}
          {{ wrap_with_time_ingestion_partitioning(build_partition_time_exp(partition_by), sql, True) }}
          {%- else -%}
          {{sql}}
          {%- endif -%}
        )
      {%- endset -%}

      {#-- Because we're putting the model SQL _directly_ into the MERGE statement,
         we need to prepend the MERGE statement with the user-configured sql_header,
         which may be needed to resolve that model SQL (e.g. referencing a variable or UDF in the header)
         in the "dynamic" case, we save the model SQL result as a temp table first, wherein the
         sql_header is included by the create_table_as macro.
      #}
      {{ get_insert_overwrite_merge_sql(target_relation, source_sql, dest_columns, [predicate], include_sql_header=true) }}

  {% else %} {# dynamic #}

      {% set predicate -%}
          {{ partition_by.render_wrapped(alias='DBT_INTERNAL_DEST') }} in unnest(dbt_partitions_for_replacement)
      {%- endset %}

      {%- set source_sql -%}
      (
        select
        {% if partition_by.time_ingestion_partitioning -%}
        _PARTITIONTIME,
        {%- endif -%}
        * from {{ tmp_relation }}
      )
      {%- endset -%}

      -- generated script to merge partitions into {{ target_relation }}
      declare dbt_partitions_for_replacement array<{{ partition_by.data_type }}>;

      {# have we already created the temp table to check for schema changes? #}
      {% if not tmp_relation_exists %}
        {{ declare_dbt_max_partition(this, partition_by, sql) }}

        -- 1. create a temp table
        {{ bq_create_table_as(partition_by.time_ingestion_partitioning, True, tmp_relation, compiled_code) }}
      {% else %}
        -- 1. temp table already exists, we used it to check for schema changes
      {% endif %}

      -- 2. define partitions to update
      set (dbt_partitions_for_replacement) = (
          select as struct
              array_agg(distinct {{ partition_by.render_wrapped() }})
          from {{ tmp_relation }}
      );

      -- 3. run the merge statement
      {{ get_insert_overwrite_merge_sql(target_relation, source_sql, dest_columns, [predicate]) }};

      -- 4. clean up the temp table
      drop table if exists {{ tmp_relation }}

  {% endif %}

{% endmacro %}
