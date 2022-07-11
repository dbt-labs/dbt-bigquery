{% macro bigquery__get_show_grant_sql(relation) %}
    {% set location = adapter.get_dataset_location(relation) %}
    {% set relation = relation.incorporate(location=location) %}

    select privilege_type, grantee
    from {{ relation.information_schema("OBJECT_PRIVILEGES") }}
    where object_schema = "{{ relation.dataset }}"
      and object_name = "{{ relation.identifier }}"
      -- filter out current user
      and split(grantee, ':')[offset(1)] != session_user()
{% endmacro %}


{%- macro bigquery__get_grant_sql(relation, grant_config) -%}
    {%- set grant_statements = [] -%}
    {%- for privilege in grant_config.keys() %}
        {%- set grantees = grant_config[privilege] -%}
        {%- if grantees %}
            {% set grant_sql -%}
                grant `{{ privilege }}` on {{ relation.type }} {{ relation }} to {{ '\"' + grantees|join('\", \"') + '\"' }}
            {%- endset %}
            {%- do grant_statements.append(grant_sql) -%}
        {% endif -%}
    {%- endfor -%}
    {{ return(grant_statements) }}

{%- endmacro %}


{% macro bigquery__get_revoke_sql(relation, grant_config) %}

    {%- set revoke_statements = [] -%}
    {%- for privilege in grant_config.keys() -%}
        {%- set grantees = grant_config[privilege] -%}
        {%- if grantees %}
            {% set revoke_sql -%}
            revoke `{{ privilege }}` on {{ relation.type }} {{ relation }} from {{ '\"' + grantees|join('\", \"') + '\"' }}
            {%- endset %}
            {%- do revoke_statements.append(revoke_sql) -%}
        {% endif -%}
    {%- endfor -%}
    {{ return(revoke_statements) }}

{%- endmacro -%}
