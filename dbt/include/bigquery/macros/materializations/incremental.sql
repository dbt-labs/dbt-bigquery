{% macro declare_dbt_max_partition(relation, partition_by, complied_code, language='sql') %}

  {#-- TODO: revisit partitioning with python models --#}
  {%- if '_dbt_max_partition' in complied_code and language == 'sql' -%}

    declare _dbt_max_partition {{ partition_by.data_type }} default (
      select max({{ partition_by.field }}) from {{ this }}
      where {{ partition_by.field }} is not null
    );

  {%- endif -%}

{% endmacro %}


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


{% macro bq_insert_overwrite(
    tmp_relation, target_relation, sql, unique_key, partition_by, partitions, dest_columns, tmp_relation_exists
) %}

  {% if partitions is not none and partitions != [] %} {# static #}

      {% set predicate -%}
          {{ partition_by.render(alias='DBT_INTERNAL_DEST') }} in (
              {{ partitions | join (', ') }}
          )
      {%- endset %}

      {%- set source_sql -%}
        (
          {{sql}}
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
          {{ partition_by.render(alias='DBT_INTERNAL_DEST') }} in unnest(dbt_partitions_for_replacement)
      {%- endset %}

      {%- set source_sql -%}
      (
        select * from {{ tmp_relation }}
      )
      {%- endset -%}

      -- generated script to merge partitions into {{ target_relation }}
      declare dbt_partitions_for_replacement array<{{ partition_by.data_type }}>;

      {# have we already created the temp table to check for schema changes? #}
      {% if not tmp_relation_exists %}
        {{ declare_dbt_max_partition(this, partition_by, sql) }}

        -- 1. create a temp table
        {%- call statement('main') -%}
          {{ create_table_as(True, tmp_relation, compiled_code) }}
        {%- endcall -%}
      {% else %}
        -- 1. temp table already exists, we used it to check for schema changes
      {% endif %}

      -- 2. define partitions to update
      set (dbt_partitions_for_replacement) = (
          select as struct
              array_agg(distinct {{ partition_by.render() }})
          from {{ tmp_relation }}
      );

      -- 3. run the merge statement
      {{ get_insert_overwrite_merge_sql(target_relation, source_sql, dest_columns, [predicate]) }};

      -- 4. clean up the temp table
      drop table if exists {{ tmp_relation }}

  {% endif %}

{% endmacro %}


{% macro bq_generate_incremental_build_sql(
    strategy, tmp_relation, target_relation, sql, unique_key, partition_by, partitions, dest_columns, tmp_relation_exists
) %}
  {#-- if partitioned, use BQ scripting to get the range of partition values to be updated --#}
  {% if strategy == 'insert_overwrite' %}

    {% set missing_partition_msg -%}
      The 'insert_overwrite' strategy requires the `partition_by` config.
    {%- endset %}
    {% if partition_by is none %}
      {% do exceptions.raise_compiler_error(missing_partition_msg) %}
    {% endif %}

    {% set build_sql = bq_insert_overwrite(
        tmp_relation, target_relation, sql, unique_key, partition_by, partitions, dest_columns, tmp_relation_exists
    ) %}

  {% else %} {# strategy == 'merge' #}
    {%- set source_sql -%}
      {%- if tmp_relation_exists -%}
        (
          select * from {{ tmp_relation }}
        )
      {%- else -%} {#-- wrap sql in parens to make it a subquery --#}
        (
          {{sql}}
        )
      {%- endif -%}
    {%- endset -%}

    {% set build_sql = get_merge_sql(target_relation, source_sql, unique_key, dest_columns) %}

  {% endif %}

  {{ return(build_sql) }}

{% endmacro %}

{% materialization incremental, adapter='bigquery' -%}

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

   -- grab current tables grants config for comparision later on
  {% set grant_config = config.get('grants') %}

  {{ run_hooks(pre_hooks) }}

  {% if existing_relation is none %}
      {%- call statement('main', language=language) -%}
        {{ create_table_as(False, target_relation, compiled_code, language) }}
      {%- endcall -%}

  {% elif existing_relation.is_view %}
      {#-- There's no way to atomically replace a view with a table on BQ --#}
      {{ adapter.drop_relation(existing_relation) }}
      {%- call statement('main', language=language) -%}
        {{ create_table_as(False, target_relation, compiled_code, language) }}
      {%- endcall -%}

  {% elif full_refresh_mode %}
      {#-- If the partition/cluster config has changed, then we must drop and recreate --#}
      {% if not adapter.is_replaceable(existing_relation, partition_by, cluster_by) %}
          {% do log("Hard refreshing " ~ existing_relation ~ " because it is not replaceable") %}
          {{ adapter.drop_relation(existing_relation) }}
      {% endif %}
      {%- call statement('main', language=language) -%}
        {{ create_table_as(False, target_relation, compiled_code, language) }}
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
           create_table_as(True, tmp_relation, compiled_code, language)
        }}
      {%- endcall -%}
      {% set tmp_relation_exists = true %}
      {#-- Process schema changes. Returns dict of changes if successful. Use source columns for upserting/merging --#}
      {% set dest_columns = process_schema_changes(on_schema_change, tmp_relation, existing_relation) %}
    {% endif %}

    {% if not dest_columns %}
      {% set dest_columns = adapter.get_columns_in_relation(existing_relation) %}
    {% endif %}

    {% set build_sql = bq_generate_incremental_build_sql(
        strategy, tmp_relation, target_relation, compiled_code, unique_key, partition_by, partitions, dest_columns, tmp_relation_exists
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
