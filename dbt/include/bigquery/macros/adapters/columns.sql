{% macro bigquery__get_empty_subquery_sql(select_sql, select_sql_header=none) %}
    {%- if select_sql_header is not none -%}
    {{ select_sql_header }}
    {%- endif -%}
    {%- set contract_config = config.get('contract') -%}
    {%- if contract_config.enforced and '_dbt_max_partition' in compiled_code -%}
        {%- set raw_partition_by = config.get('partition_by', none) -%}
        {%- set partition_by = adapter.parse_partition_by(raw_partition_by) -%}

        declare _dbt_max_partition {{ partition_by.data_type_for_partition() }} default (
        select max({{ partition_by.field }}) from {{ relation }}
        where {{ partition_by.field }} is not null );
    {%- endif -%}
    select * from (
        {{ select_sql }}
    ) as __dbt_sbq
    where false and current_timestamp() = current_timestamp()
    limit 0
{% endmacro %}
