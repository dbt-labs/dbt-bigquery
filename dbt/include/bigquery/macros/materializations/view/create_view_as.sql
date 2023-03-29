{% macro bigquery__create_view_as(relation, sql) %}
    {% set sql_header = config.get('sql_header', none) %}

    {{ sql_header if sql_header is not none }}

    create or replace view {{ relation }}
        {{ bigquery_view_options(config, model) }}
        {% set contract_config = config.get('contract') %}
        {% if contract_config.enforced %}
        {{ get_assert_columns_equivalent(sql) }}
        {% endif %}
    as {{ sql }};

{% endmacro %}


{% macro bigquery__handle_existing_table(full_refresh, old_relation) %}
    {% if full_refresh %}
        {{ adapter.drop_relation(old_relation) }}
    {% else %}
        {{ exceptions.relation_wrong_type(old_relation, 'view') }}
    {% endif %}
{% endmacro %}


{% macro bigquery_view_options(config, node) %}
    {% set opts = adapter.get_view_options(config, node) %}
    {% do return(bigquery_options(opts)) %}
{% endmacro %}
