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

{%- macro get_max_partition(relation, data_type=none) -%}
    {%- if not execute -%}
        {{- return(none) -}}
    {%- endif -%}

    {%- if data_type is none -%}
        {# Get info about partitioning. #}
        {%- set raw_partition_by = config.get('partition_by', none) -%}
        {%- set partition_config = adapter.parse_partition_by(raw_partition_by) -%}
        {%- set data_type = partition_config.data_type -%}
    {%- endif -%}

    {# Get the maxiumum partition id. #}
    {%- set partition_id = get_partitions_metadata(relation)| selectattr('total_rows', 'gt', 0) | selectattr('partition_id', 'ne', '__NULL__') | map(attribute='partition_id') | max -%}
    {# If partition_id == '' then there are no partitions with data which aren't the __NULL__ partition #}
    {%- if partition_id == '' -%}
        {{- return(none) -}}
    {%- endif -%}

    {# Format partition id for SQL comparision. #}
    {%- if data_type | lower in ('date', 'timestamp', 'datetime') -%}
        {%- if partition_id | length == 4 -%}
            {%- set format = '%Y' -%}
        {%- elif partition_id | length == 6 -%}
            {%- set format = '%Y%m' -%}
        {%- elif partition_id | length == 8 -%}
            {%- set format = '%Y%m%d' -%}
        {%- else -%}
            {%- set format = '%Y%m%d%H' -%}
        {%- endif -%}
        {%- set res = "parse_timestamp('{}', '{}')".format(format, partition_id) -%}
    {%- else -%}
        {%- set res = "'{}'".format(partition_id) -%}
    {%- endif -%}
    {{- return(safe_cast(res, data_type)) -}}
{%- endmacro -%}
