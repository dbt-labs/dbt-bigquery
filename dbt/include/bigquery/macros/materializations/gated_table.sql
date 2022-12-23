/*
    Noah Kennedy | noah.kennedy
    12/23/2022 - gated_table macro logic
    This is a custom materialization that requires a config parameter for gating_logic statements.
    Config parameters are passed in as a list - see example below
    It materializes two tables instead of one - the first table is a normal table materialization
    The second table that's created is called `gated_failure__{table_name}`.
    These tables combined are mutually exclusive, completely exhaustive (MECE)
    The first table is all rows that pass all logical statements defined in the gating_logic, the second table is all rows that fail any piece of gating logic.

    Example Config block -
        {{ config(materialized='gated_table',
                tags=['gated_table'],
                gating_logic=['id is not null'
                , 'field1 != 1']
            )
        }}
 */


-- The below macro separates out the list of gating logic statements into WHERE clause syntax.
    -- In the top loop, if it's a production model, we want to pull in any row that passes all filters (AND).
    -- In the bottom loop, if it's a failures model, we want to pull in any row that fails any filter (OR)
{% macro form_where_clause(gating_logic, is_production_model) %}
    {% if is_production_model %}
        {% for logical_statement in gating_logic %}
            {% if loop.first %}
                {{ logical_statement }}
            {% else %}
                AND {{ logical_statement }}
            {% endif %}
        {% endfor %}
    {% else %}
        {% for logical_statement in gating_logic %}
            {% if loop.first %}
                NOT {{ logical_statement }}
            {% else %}
                OR NOT {{ logical_statement }}
            {% endif %}
        {% endfor %}
    {% endif %}
{% endmacro %}

-- The below macro separates out the list of gating logic statements into a SELECT statement for the failures table.
    -- This gives metadata so users can see exactly which logical statement failed in the __gated_failures table
{% macro form_failure_select_statement(gating_logic) %}
    {% set num_gates = gating_logic | length %}
    , {{ num_gates }} as num_gates
    {% for logical_statement in gating_logic %}
        , cast("""{{ logical_statement }}""" as STRING) as TEST_DEFINITION_{{ loop.index }}
        , cast({{ logical_statement }} as BOOLEAN) as TEST_PASSING_{{ loop.index }}
    {% endfor %}
{% endmacro %}

-- The below macro creates the gated sql statements for both models, production and failures, using the above macros.
-- If it's a production model we want the SQL with the where clause.
-- If it's a failures model, we want to inject the filters into the SELECTm statement to see how it fails.
{% macro get_gated_sql(sql, gating_logic, is_production_model) %}
    {% if is_production_model %}
        select *
        from (
                 {{sql}}
                 )
        where {{ form_where_clause(gating_logic, is_production_model) }}
    {% else %}
        select *
        {{ form_failure_select_statement(gating_logic) }}
        from (
                 {{sql}}
                 )
        where {{ form_where_clause(gating_logic, is_production_model) }}
    {% endif %}

{% endmacro %}

{%- materialization gated_table, adapter='bigquery' -%}
    {%- set gating_logic = config.require('gating_logic') -%}

    {%- set identifier = model['alias'] -%}
    {%- set gate_failures_identifier = 'gate_failures__' + model['name'] -%}

    {%- set old_relation = adapter.get_relation(database=database, schema=schema, identifier=identifier) -%}
    {%- set target_relation = api.Relation.create(database=database, schema=schema, identifier=identifier, type='table') -%}
    {%- set gate_failures_relation = api.Relation.create(database=database, schema=schema, identifier=gate_failures_identifier, type='table') -%}

    {%- set exists_not_as_table = (old_relation is not none and not old_relation.is_table) -%}

    {{ run_hooks(pre_hooks) }}

    {%- if exists_not_as_table -%}
            {{ adapter.drop_relation(old_relation) }}
    {%- endif -%}

      -- build model
    {%- set raw_partition_by = config.get('partition_by', none) -%}
    {%- set partition_by = adapter.parse_partition_by(raw_partition_by) -%}
    {%- set cluster_by = config.get('cluster_by', none) -%}

    {% if not adapter.is_replaceable(old_relation, partition_by, cluster_by) %}
        {% do log("Hard refreshing " ~ old_relation ~ " because it is not replaceable") %}
        {% do adapter.drop_relation(old_relation) %}
    {% endif %}

    -- Create model as normal
    {% call statement('main') -%}
        {%- set passing_sql = get_gated_sql(sql, gating_logic, True) %}
        {{create_table_as(False, target_relation, passing_sql)}}
    {%- endcall %}

     -- Send failures to failures table
    {%call statement('failures') -%}
        {%- set failing_sql = get_gated_sql(sql, gating_logic, False) %}
        {{create_table_as(False, gate_failures_relation, failing_sql)}}
    {%- endcall %}

  {{ run_hooks(post_hooks) }}

  {% do persist_docs(target_relation, model) %}

  {{ return({'relations': [target_relation]}) }}

{% endmaterialization %}