{% materialization udf, adapter='bigquery' %}
    {%- set target_relation = this %}

    {{ run_hooks(pre_hooks) }}

    -- create the UDF
    {%- call statement('main') -%}
        {{ get_create_udf_as_sql(target_relation, sql) }}
    {%- endcall -%}

    {{ run_hooks(post_hooks) }}

    -- TODO - grants

    {{ return({'relations': [target_relation]}) }}
{% endmaterialization %}
