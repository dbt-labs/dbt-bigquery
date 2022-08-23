{% macro date_sharded_table(base_name) %}
    {{ return(base_name ~ "[DBT__PARTITION_DATE]") }}
{% endmacro %}

{% macro grant_access_to(entity, entity_type, role, grant_target_dict) -%}
  {% do adapter.grant_access_to(entity, entity_type, role, grant_target_dict) %}
{% endmacro %}

{%- macro get_partitions_metadata(table) -%}
  {%- if execute -%}
    {%- set res = adapter.get_partitions_metadata(table) -%}
    {{- return(res) -}}
  {%- endif -%}
  {{- return(None) -}}
{%- endmacro -%}

{%- macro get_max_partition(relation) -%}
    {%- if not execute -%}
        {{- return(none) -}}
    {%- endif -%}

    {# Get info about partitioning. #}
    {%- set raw_partition_by = config.get('partition_by', none) -%}
    {%- set partition_config = adapter.parse_partition_by(raw_partition_by) -%}

    {# Get the maxiumum partition id. #}
    {%- set partitions_metadata = get_partitions_metadata(relation) -%}
    {%- set partition_id = partitions_metadata.columns['partition_id'].values() | max -%}

    {# Format partition id for SQL comparision. #}
    {%- if partition_config.data_type | lower in ('date', 'timestamp', 'datetime') -%}
        {%- if partition_id | length == 4 -%}
            {%- set format = '%Y' -%}
        {%- elif partition_id | length == 6 -%}
            {%- set format = '%Y%m' -%}
        {%- elif partition_id | length == 8 -%}
            {%- set format = '%Y%m%d' -%}
        {%- else -%}
            {%- set format = '%Y%m%d%H' -%}
        {%- endif -%}
        {%- set res = dbt.safe_cast("parse_timestamp('{}', '{}')".format(format, partition_id), partition_config.data_type) -%}
    {%- else -%}
        {%- set res = dbt.safe_cast("'{}'".format(partition_id), dbt.type_int()) -%}
    {%- endif -%}
    {{- return(res) -}}
{%- endmacro -%}
