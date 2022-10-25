{% macro wrap_with_time_ingestion_partitioning_sql(partition_time_exp, sql, is_nested) %}

  select {{ partition_time_exp['value'] }} as _partitiontime, * EXCEPT({{ partition_time_exp['field'] }}) from (
    {{ sql }}
  ){%- if not is_nested -%};{%- endif -%}

{% endmacro %}

{% macro create_ingestion_time_partitioned_table_as_sql(temporary, relation, sql) -%}
  {%- set raw_partition_by = config.get('partition_by', none) -%}
  {%- set raw_cluster_by = config.get('cluster_by', none) -%}
  {%- set sql_header = config.get('sql_header', none) -%}

  {%- set partition_config = adapter.parse_partition_by(raw_partition_by) -%}

  {%- set columns = get_columns_with_types_in_query_sql(sql) -%}
  {%- set table_dest_columns_csv = columns_without_partition_fields_csv(partition_config, columns) -%}

  {{ sql_header if sql_header is not none }}

  {% set ingestion_time_partition_config_raw = fromjson(tojson(raw_partition_by)) %}
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

{% macro bq_insert_into_ingestion_time_partitioned_table_sql(target_relation, sql) -%}
  {%- set partition_by = config.get('partition_by', none) -%}
  {% set dest_columns = adapter.get_columns_in_relation(target_relation) %}
  {%- set dest_columns_csv = get_quoted_csv(dest_columns | map(attribute="name")) -%}

  insert into {{ target_relation }} (_partitiontime, {{ dest_columns_csv }})
    {{ wrap_with_time_ingestion_partitioning_sql(build_partition_time_exp(partition_by), sql, False) }}

{%- endmacro -%}

{% macro get_columns_with_types_in_query_sql(select_sql) %}
  {% set sql %}
    select * from (
      {{ select_sql }}
    ) as __dbt_sbq
    where false
    limit 0
  {% endset %}
  {{ return(adapter.get_columns_in_select_sql(sql)) }}
{% endmacro %}
