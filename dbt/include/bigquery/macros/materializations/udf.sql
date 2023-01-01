{% materialization udf, adapter='bigquery' %}
    {%- set target_relation = this %}

    {{ run_hooks(pre_hooks) }}

    -- Create the UDF
    {%- call statement('main') -%}
        {{ bigquery__get_create_udf_as_sql(target_relation, sql) }}
    {%- endcall -%}

    {{ run_hooks(post_hooks) }}

    -- We do not need to explicitly call persist_docs as in other
    -- materializations because UDF documentation is handled in the
    -- get_create_udf_as_sql macro. There is no concept of column-level
    -- documentation for UDFs.

    -- Not calling apply_grants because dataset-level grants not
    -- yet natively supported in dbt, and BigQuery does not support
    -- permissions at the level of individual UDFs

    {{ return({'relations': [target_relation]}) }}
{% endmaterialization %}
