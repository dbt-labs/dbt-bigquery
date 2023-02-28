{% macro dbt_bigquery_validate_get_incremental_strategy(config) %}
  {#-- Find and validate the incremental strategy #}
  {%- set strategy = config.get("incremental_strategy") or 'merge' -%}

  {% set invalid_strategy_msg -%}
    Invalid incremental strategy provided: {{ strategy }}
    Expected one of: 'merge', 'insert_overwrite'
  {%- endset %}
  {% if strategy not in ['merge', 'insert_overwrite'] %}
    {% do exceptions.raise_compiler_error(invalid_strategy_msg) %}
  {% endif %}

  {% do return(strategy) %}
{% endmacro %}

{% macro source_sql_with_partition(partition_by, source_sql) %}

  {%- if partition_by.time_ingestion_partitioning %}
    {{ return(wrap_with_time_ingestion_partitioning_sql(build_partition_time_exp(partition_by.field), source_sql, False))  }}
  {% else %}
    {{ return(source_sql)  }}
  {%- endif -%}

{% endmacro %}
{% macro bq_create_table_as(is_time_ingestion_partitioning, temporary, relation, compiled_code, language='sql') %}
  {% if is_time_ingestion_partitioning %}
    {#-- Create the table before inserting data as ingestion time partitioned tables can't be created with the transformed data --#}
    {% do run_query(create_ingestion_time_partitioned_table_as_sql(temporary, relation, sql)) %}
    {{ return(bq_insert_into_ingestion_time_partitioned_table_sql(relation, sql)) }}
  {% else %}
    {{ return(create_table_as(temporary, relation, sql)) }}
  {% endif %}
{% endmacro %}

{% macro bq_generate_incremental_build_sql(
    strategy, tmp_relation, target_relation, sql, unique_key, partition_by, partitions, dest_columns, tmp_relation_exists, copy_partitions, incremental_predicates
) %}
  {#-- if partitioned, use BQ scripting to get the range of partition values to be updated --#}
  {% if strategy == 'insert_overwrite' %}

    {% set build_sql = bq_generate_incremental_insert_overwrite_build_sql(
        tmp_relation, target_relation, sql, unique_key, partition_by, partitions, dest_columns, tmp_relation_exists, copy_partitions
    ) %}

  {% else %} {# strategy == 'merge' #}

    {% set build_sql = bq_generate_incremental_merge_build_sql(
        tmp_relation, target_relation, sql, unique_key, partition_by, dest_columns, tmp_relation_exists, incremental_predicates
    ) %}

  {% endif %}

  {{ return(build_sql) }}

{% endmacro %}

{% materialization incremental, adapter='bigquery', supported_languages=['sql', 'python'] -%}

  {%- set unique_key = config.get('unique_key') -%}
  {%- set full_refresh_mode = (should_full_refresh()) -%}
  {%- set language = model['language'] %}

  {%- set target_relation = this %}
  {%- set existing_relation = load_relation(this) %}
  {%- set tmp_relation = make_temp_relation(this) %}

  {#-- Validate early so we don't run SQL if the strategy is invalid --#}
  {% set strategy = dbt_bigquery_validate_get_incremental_strategy(config) -%}

  {%- set raw_partition_by = config.get('partition_by', none) -%}
  {%- set partition_by = adapter.parse_partition_by(raw_partition_by) -%}
  {%- set partitions = config.get('partitions', none) -%}
  {%- set cluster_by = config.get('cluster_by', none) -%}

  {% set on_schema_change = incremental_validate_on_schema_change(config.get('on_schema_change'), default='ignore') %}
  {% set incremental_predicates = config.get('predicates', default=none) or config.get('incremental_predicates', default=none) %}

   -- grab current tables grants config for comparison later on
  {% set grant_config = config.get('grants') %}

  {{ run_hooks(pre_hooks) }}

  {% if partition_by.copy_partitions is true and strategy != 'insert_overwrite' %} {#-- We can't copy partitions with merge strategy --#}
        {% set wrong_strategy_msg -%}
        The 'copy_partitions' option requires the 'incremental_strategy' option to be set to 'insert_overwrite'.
        {%- endset %}
        {% do exceptions.raise_compiler_error(wrong_strategy_msg) %}

  {% elif existing_relation is none %}
      {%- call statement('main', language=language) -%}
        {{ bq_create_table_as(partition_by.time_ingestion_partitioning, False, target_relation, compiled_code, language) }}
      {%- endcall -%}

  {% elif existing_relation.is_view %}
      {#-- There's no way to atomically replace a view with a table on BQ --#}
      {{ adapter.drop_relation(existing_relation) }}
      {%- call statement('main', language=language) -%}
        {{ bq_create_table_as(partition_by.time_ingestion_partitioning, False, target_relation, compiled_code, language) }}
      {%- endcall -%}

  {% elif full_refresh_mode %}
      {#-- If the partition/cluster config has changed, then we must drop and recreate --#}
      {% if not adapter.is_replaceable(existing_relation, partition_by, cluster_by) %}
          {% do log("Hard refreshing " ~ existing_relation ~ " because it is not replaceable") %}
          {{ adapter.drop_relation(existing_relation) }}
      {% endif %}
      {%- call statement('main', language=language) -%}
        {{ bq_create_table_as(partition_by.time_ingestion_partitioning, False, target_relation, compiled_code, language) }}
      {%- endcall -%}

  {% else %}
    {%- if language == 'python' and strategy == 'insert_overwrite' -%}
      {#-- This lets us move forward assuming no python will be directly templated into a query --#}
      {%- set python_unsupported_msg -%}
        The 'insert_overwrite' strategy is not yet supported for python models.
      {%- endset %}
      {% do exceptions.raise_compiler_error(python_unsupported_msg) %}
    {%- endif -%}

    {% set tmp_relation_exists = false %}
    {% if on_schema_change != 'ignore' or language == 'python' %}
      {#-- Check first, since otherwise we may not build a temp table --#}
      {#-- Python always needs to create a temp table --#}
      {%- call statement('create_tmp_relation', language=language) -%}
        {{ declare_dbt_max_partition(this, partition_by, compiled_code, language) +
           bq_create_table_as(partition_by.time_ingestion_partitioning, True, tmp_relation, compiled_code, language)
        }}
      {%- endcall -%}
      {% set tmp_relation_exists = true %}
      {#-- Process schema changes. Returns dict of changes if successful. Use source columns for upserting/merging --#}
      {% set dest_columns = process_schema_changes(on_schema_change, tmp_relation, existing_relation) %}
    {% endif %}

    {% if not dest_columns %}
      {% set dest_columns = adapter.get_columns_in_relation(existing_relation) %}
    {% endif %}
    {% if partition_by.time_ingestion_partitioning %}
      {% set dest_columns = adapter.add_time_ingestion_partition_column(dest_columns) %}
    {% endif %}
    {% set build_sql = bq_generate_incremental_build_sql(
        strategy, tmp_relation, target_relation, compiled_code, unique_key, partition_by, partitions, dest_columns, tmp_relation_exists, partition_by.copy_partitions, incremental_predicates
    ) %}

    {%- call statement('main') -%}
      {{ build_sql }}
    {% endcall %}

    {%- if language == 'python' and tmp_relation -%}
      {{ adapter.drop_relation(tmp_relation) }}
    {%- endif -%}

  {% endif %}

  {{ run_hooks(post_hooks) }}

  {% set target_relation = this.incorporate(type='table') %}

  {% set should_revoke = should_revoke(existing_relation, full_refresh_mode) %}
  {% do apply_grants(target_relation, grant_config, should_revoke) %}

  {% do persist_docs(target_relation, model) %}

  {{ return({'relations': [target_relation]}) }}

{%- endmaterialization %}
