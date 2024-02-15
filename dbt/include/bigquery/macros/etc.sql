{% macro date_sharded_table(base_name) %}
    {{ return(base_name ~ "[DBT__PARTITION_DATE]") }}
{% endmacro %}

{% macro grant_access_to(entity, entity_type, role, grant_target_dict) -%}
  {% do adapter.grant_access_to(entity, entity_type, role, grant_target_dict) %}
{% endmacro %}

{#
  This macro returns the partition metadata for provided table.
  The expected input is a table object (ie through a `source` or `ref`).
  The output contains the result from partitions information for your input table.
  The details of the retrieved columns can be found on https://cloud.google.com/bigquery/docs/managing-partitioned-tables
  It will leverage the INFORMATION_SCHEMA.PARTITIONS table.
#}
{%- macro get_partitions_metadata(table) -%}
  {%- if execute -%}
    {%- set res = adapter.get_partitions_metadata(table) -%}
    {{- return(res) -}}
  {%- endif -%}
  {{- return(None) -}}
{%- endmacro -%}
