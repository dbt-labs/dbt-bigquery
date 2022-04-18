{% macro declare_dbt_max_partition(relation, partition_by, sql) %}

  {% if '_dbt_max_partition' in sql %}

    declare _dbt_max_partition {{ partition_by.data_type }} default (
      select max({{ partition_by.field }}) from {{ this }}
      where {{ partition_by.field }} is not null
    );

  {% endif %}

{% endmacro %}


{% macro dbt_bigquery_validate_get_incremental_strategy(config) %}
  {#-- Find and validate the incremental strategy #}
  {%- set strategy = config.get("incremental_strategy", default="merge") -%}

  {% set invalid_strategy_msg -%}
    Invalid incremental strategy provided: {{ strategy }}
    Expected one of: 'merge', 'insert_overwrite'
  {%- endset %}
  {% if strategy not in ['merge', 'insert_overwrite'] %}
    {% do exceptions.raise_compiler_error(invalid_strategy_msg) %}
  {% endif %}

  {% do return(strategy) %}
{% endmacro %}

{% macro get_columns_with_types_in_query(select_sql) %}
  {% set sql %}
    select * from (
      {{ select_sql }}
    ) as __dbt_sbq
    where false
    limit 0
  {% endset %}
  {{ return(adapter.get_columns_in_select_sql(sql)) }}
{% endmacro %}

{% macro create_ingestion_time_partitioned_table_as(temporary, relation, sql) -%}
  {%- set raw_partition_by = config.get('partition_by', none) -%}
  {%- set raw_cluster_by = config.get('cluster_by', none) -%}
  {%- set sql_header = config.get('sql_header', none) -%}

  {%- set partition_config = adapter.parse_partition_by(raw_partition_by) -%}

  {%- set columns = get_columns_with_types_in_query(sql) -%}
  {%- set table_dest_columns_csv = columns_without_partition_fields_csv(partition_config, columns) -%}

  {{ sql_header if sql_header is not none }}

  {% set ingestion_time_partition_config_raw = fromjson(tojson(raw_partition_by))  %}
  {% do ingestion_time_partition_config_raw.update({'field':'_PARTITIONTIME'}) %}

  {%- set ingestion_time_partition_config = adapter.parse_partition_by(ingestion_time_partition_config_raw) -%}

  create or replace table {{ relation }} ({{table_dest_columns_csv}})
  {{ partition_by(ingestion_time_partition_config) }}
  {{ cluster_by(raw_cluster_by) }}
  {{ bigquery_table_options(config, model, temporary) }}

{%- endmacro -%}

{% macro get_quoted_with_types_csv(columns) %}
    {% set quoted = [] %}
    {% for col in columns -%}
        {%- do quoted.append(adapter.quote(col.name) ~ " " ~ col.data_type) -%}
    {%- endfor %}
    {%- set dest_cols_csv = quoted | join(', ') -%}
    {{ return(dest_cols_csv) }}

{% endmacro %}

{% macro columns_without_partition_fields_csv(partition_config, columns) -%}
  {%- set columns_no_partition = partition_config.reject_partition_field_column(columns) -%}
  {% set columns_names = get_quoted_with_types_csv(columns_no_partition) %}
  {{ return(columns_names) }}

{%- endmacro -%}

{% macro bq_insert_into_ingestion_time_partitioned_table(target_relation, sql) -%}
  {%- set partition_by = config.get('partition_by', none) -%}
  {% set dest_columns = adapter.get_columns_in_relation(target_relation) %}
  {%- set dest_columns_csv = get_quoted_csv(dest_columns | map(attribute="name")) -%}

  insert into {{ target_relation }} (_partitiontime, {{ dest_columns_csv }})
    {{ wrap_with_time_ingestion_partitioning(build_partition_time_exp(partition_by), sql, False) }}

{%- endmacro -%}

{% macro  build_partition_time_exp(partition_by) %}
  {% if partition_by.data_type == 'timestamp' %}
    {% set partition_value = partition_by.field %}
  {% else %}
    {% set partition_value = 'timestamp(' + partition_by.field + ')' %}
  {% endif %}
  {{ return({'value': partition_value, 'field': partition_by.field}) }}
{% endmacro %}

{% macro  wrap_with_time_ingestion_partitioning(partition_time_exp, sql, is_nested) %}

  select {{ partition_time_exp['value'] }} as _partitiontime, * EXCEPT({{ partition_time_exp['field'] }}) from (
    {{ sql }}
  ){%- if not is_nested -%};{%- endif -%}

{% endmacro %}

{% macro source_sql_with_partition(partition_by, source_sql) %}

  {%- if partition_by.time_ingestion_partitioning %}
    {{ return(wrap_with_time_ingestion_partitioning(build_partition_time_exp(partition_by.field), source_sql, False))  }}
  {% else %}
    {{ return(source_sql)  }}
  {%- endif -%}

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

      declare dbt_partitions_for_replacement array<{{ partition_by.data_type }}>;

      {# have we already created the temp table to check for schema changes? #}
      {% if not tmp_relation_exists %}
        {{ declare_dbt_max_partition(this, partition_by, sql) }}

        -- 1. create a temp table
        {% set create_table_sql = bq_create_table_as(partition_by.time_ingestion_partitioning, True, tmp_relation, sql) %}
        {{ create_table_sql }}
      {% else %}
        -- 1. temp table already exists, we used it to check for schema changes
      {% endif %}

      -- 2. define partitions to update
      set (dbt_partitions_for_replacement) = (
          select as struct
              array_agg(distinct {{ partition_by.render_wrapped() }})
          from {{ tmp_relation }}
      );

      {#
        TODO: include_sql_header is a hack; consider a better approach that includes
              the sql_header at the materialization-level instead
      #}
      -- 3. run the merge statement
      {{ get_insert_overwrite_merge_sql(target_relation, source_sql, dest_columns, [predicate], include_sql_header=false) }};

      -- 4. clean up the temp table
      drop table if exists {{ tmp_relation }}

  {% endif %}

{% endmacro %}

{% macro bq_create_table_as(is_time_ingestion_partitioning, temporary, relation, sql) %}
  {% if is_time_ingestion_partitioning %}
    {#-- Create the table before inserting data as ingestion time partitioned tables can't be created with the transformed data --#}
    {% do run_query(create_ingestion_time_partitioned_table_as(temporary, relation, sql)) %}
    {{ return(bq_insert_into_ingestion_time_partitioned_table(relation, sql)) }}
  {% else %}
    {{ return(create_table_as(temporary, relation, sql)) }}
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
        tmp_relation, target_relation, sql, unique_key, partition_by, partitions, dest_columns, on_schema_change
    ) %}

  {% else %} {# strategy == 'merge' #}
    {%- set source_sql -%}
      {%- if tmp_relation_exists -%}
        (
        select
        {% if partition_by.time_ingestion_partitioning -%}
        _PARTITIONTIME,
        {%- endif -%}
        * from {{ tmp_relation }}
      )
      {%- else -%} {#-- wrap sql in parens to make it a subquery --#}
        (
          {%- if partition_by.time_ingestion_partitioning -%}
          {{ wrap_with_time_ingestion_partitioning(build_partition_time_exp(partition_by), sql, True) }}
          {%- else -%}
          {{sql}}
          {%- endif -%}
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

  {{ run_hooks(pre_hooks) }}

  {% if existing_relation is none %}
      {% set build_sql = bq_create_table_as(partition_by.time_ingestion_partitioning, False, target_relation, sql) %}

  {% elif existing_relation.is_view %}
      {#-- There's no way to atomically replace a view with a table on BQ --#}
      {{ adapter.drop_relation(existing_relation) }}
      {% set build_sql = bq_create_table_as(partition_by.time_ingestion_partitioning, False, target_relation, sql) %}

  {% elif full_refresh_mode %}
      {#-- If the partition/cluster config has changed, then we must drop and recreate --#}
      {% if not adapter.is_replaceable(existing_relation, partition_by, cluster_by) %}
          {% do log("Hard refreshing " ~ existing_relation ~ " because it is not replaceable") %}
          {{ adapter.drop_relation(existing_relation) }}
      {% endif %}
      {% set build_sql = bq_create_table_as(partition_by.time_ingestion_partitioning, False, target_relation, sql) %}

  {% else %}
    {% set tmp_relation_exists = false %}
    {% if on_schema_change != 'ignore' %} {# Check first, since otherwise we may not build a temp table #}
      {% do run_query(
        declare_dbt_max_partition(this, partition_by, sql) + bq_create_table_as(partition_by.time_ingestion_partitioning, True, tmp_relation, sql)
      ) %}
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
        strategy, tmp_relation, target_relation, sql, unique_key, partition_by, partitions, dest_columns, tmp_relation_exists
    ) %}

  {% endif %}

  {%- call statement('main') -%}
    {{ build_sql }}
  {% endcall %}

  {{ run_hooks(post_hooks) }}

  {% set target_relation = this.incorporate(type='table') %}

  {% do persist_docs(target_relation, model) %}

  {{ return({'relations': [target_relation]}) }}

{%- endmaterialization %}
