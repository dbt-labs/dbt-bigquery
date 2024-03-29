{% macro bigquery__can_clone_table() %}
    {{ return(True) }}
{% endmacro %}

{% macro bigquery__create_or_replace_clone(this_relation, defer_relation) %}
    {%- set full_refresh_mode = (should_full_refresh()) -%}
    {%- set raw_partition_by = config.get('partition_by', none) -%}
    {%- set partition_by = adapter.parse_partition_by(raw_partition_by) -%}
    {%- set partitions = config.get('partitions', none) -%}
    {%- set cluster_by = config.get('cluster_by', none) -%}
    
    {% if not adapter.is_replaceable(this_relation, partition_by, cluster_by) %}
        {% do log("Hard refreshing " ~ this_relation ~ " because it is not replaceable") %}
        {{ adapter.drop_relation(this_relation) }}
    {% endif %}
   
    create or replace
      table {{ this_relation }}
      clone {{ defer_relation }}
{% endmacro %}
