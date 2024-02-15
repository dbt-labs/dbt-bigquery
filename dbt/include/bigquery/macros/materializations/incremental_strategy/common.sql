{% macro declare_dbt_max_partition(relation, partition_by, compiled_code, language='sql') %}

  {#-- TODO: revisit partitioning with python models --#}
  {%- if '_dbt_max_partition' in compiled_code and language == 'sql' -%}
    {%- if partition_by.partition_information == "information_schema" -%}
      {{ dbt_max_partition_from_information_schema_data_sql(relation, partition_by) }}
    {%- else -%}
      {{ dbt_max_partition_from_model_data_sql(relation, partition_by) }}
    {%- endif -%}

  {%- endif -%}

{% endmacro %}

{% macro dbt_max_partition_from_model_data_sql(relation, partition_by) %}
  declare _dbt_max_partition {{ partition_by.data_type_for_partition() }} default (
      select max({{ partition_by.field }}) from {{ relation }}
      where {{ partition_by.field }} is not null
  );
{% endmacro %}

{% macro max_partition_wrapper(field) %}
  MAX({{ field }}) AS max_partition
{% endmacro %}

{% macro array_distinct_partition_wrapper(field) %}
  as struct
     -- IGNORE NULLS: this needs to be aligned to _dbt_max_partition, which ignores null
     array_agg(distinct {{ field }} IGNORE NULLS)
{% endmacro %}

{% macro dbt_max_partition_from_information_schema_data_sql(relation, partition_by) %}
  declare _dbt_max_partition {{ partition_by.data_type_for_partition() }} default (
      {{ partition_from_information_schema_data_sql(relation, partition_by, max_partition_wrapper) }}
  );
{% endmacro %}

{% macro partition_from_model_data_sql(relation, partition_by, field_function) %}
  select {{ field_function(partition_by.render_wrapped()) }}
  from {{ relation }}
{% endmacro %}

{% macro partition_from_information_schema_data_sql(relation, partition_by, field_function) %}

  {%- set data_type = partition_by.data_type -%}
  {%- set granularity = partition_by.granularity -%}

  {# Format partition_id to match the declared variable type #}
    {%- if data_type | lower in ('date', 'timestamp', 'datetime') -%}
        {# Datetime using time partitioning require timestamp #}
        {%- if partition_by.time_ingestion_partitioning and partition_by.data_type == 'datetime' -%}
            {%- set data_type = 'timestamp' -%}
        {%- endif -%}
        {%- if granularity == "day" -%}
            {%- set format = "%Y%m%d" -%}
        {%- else -%}
            {%- set format = "%Y%m%d%H" -%}
        {%- endif -%}
        {%- set field = "parse_"  ~ data_type ~ "('" ~ format ~ "', partition_id)" -%}
    {%- else -%}
        {%- set field = "CAST(partition_id AS INT64)" -%}
  {%- endif -%}

  SELECT {{ field_function(field) }}
  FROM `{{relation.project}}.{{relation.dataset}}.INFORMATION_SCHEMA.PARTITIONS`
  WHERE TABLE_NAME = '{{relation.identifier}}'
  AND NOT(STARTS_WITH(partition_id, "__"))

{% endmacro %}
