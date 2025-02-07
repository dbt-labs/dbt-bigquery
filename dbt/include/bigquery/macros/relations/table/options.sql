{% macro bigquery_table_options(config, node, temporary) %}
  {% set opts = adapter.get_table_options(config, node, temporary) %}
  {%- do return(bigquery_options(opts)) -%}
{%- endmacro -%}

{% macro bigquery_iceberg_table_options(config, relation) %}
  {% set base_location = config.get('base_location') %}
  {%- if not base_location-%}
      {% do exceptions.raise_compiler_error("base_location not found") %}
  {% endif %}
  {% set sub_path = relation.identifier %}
  {% set storage_uri = base_location~'/'~sub_path %}
  {% set opts = {'file_format':'"parquet"',
                 'table_format':'"iceberg"',
                'storage_uri':'"'~storage_uri~'"' } 
  %}
  {%- do return(bigquery_options(opts)) -%}
{%- endmacro -%}

{% macro bigquery_iceberg_connection(config) %}
  {% set connection = config.get('bl_connection') %}
  {%- if not connection-%}
    {% do exceptions.raise_compiler_error("BigLake connection not found") %}
  {% endif %}
  {%- do return("WITH CONNECTION `"~connection~"`") %}
{%- endmacro -%}
