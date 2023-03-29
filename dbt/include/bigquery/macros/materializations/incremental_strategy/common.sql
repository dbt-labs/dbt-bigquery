{% macro build_partition_time_exp(partition_by) %}
  {% if partition_by.data_type == 'timestamp' %}
    {% set partition_value = partition_by.field %}
  {% else %}
    {% set partition_value = 'timestamp(' + partition_by.field + ')' %}
  {% endif %}
  {{ return({'value': partition_value, 'field': partition_by.field}) }}
{% endmacro %}

{% macro declare_dbt_max_partition(relation, partition_by, compiled_code, language='sql') %}
    {#-- TODO: revisit partitioning with python models --#}
    {% if '_dbt_max_partition' in compiled_code and language == 'sql' %}
        declare _dbt_max_partition {{ partition_by.data_type_for_partition() }} default (
            select max({{ partition_by.field }})
            from {{ this }}
            where {{ partition_by.field }} is not null
        );
    {% endif %}
{% endmacro %}


{% macro source_sql_with_partition(partition_by, source_sql) %}
    {% if partition_by.time_ingestion_partitioning %}
        {{ return(
            wrap_with_time_ingestion_partitioning_sql(
                build_partition_time_exp(partition_by.field),
                source_sql,
                False
            )
        ) }}
    {% else %}
        {{ return(source_sql) }}
    {% endif %}
{% endmacro %}
