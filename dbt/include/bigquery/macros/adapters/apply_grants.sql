{% macro  bigquery__get_show_grant_sql(relation) %}
    {% if not target.location %}
        {{ exceptions.raise_compiler_error("In order to use the grants feature, you must specify a location ") }}
    {% endif %}

    select privilege_type, grantee from {{ relation.project }}.`region-{{ target.location }}`.INFORMATION_SCHEMA.OBJECT_PRIVILEGES
    where object_schema = "{{ relation.dataset }}" and object_name = "{{ relation.identifier }}" and split(grantee, ':')[offset(1)] != session_user()
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

{% macro bigquery__apply_grants(relation, grant_config, should_revoke=True) %}
    {% if grant_config %}
        {% if should_revoke %}
            {% set current_grants_table = run_query(get_show_grant_sql(relation)) %}
            {% set current_grants_dict = adapter.standardize_grants_dict(current_grants_table) %}
            {% set needs_granting = diff_of_two_dicts_no_lower(grant_config, current_grants_dict) %}
            {% set needs_revoking = diff_of_two_dicts_no_lower(current_grants_dict, grant_config) %}
            {% if not (needs_granting or needs_revoking) %}
                {{ log('On ' ~ relation ~': All grants are in place, no revocation or granting needed.')}}
            {% endif %}
        {% else %}
            {% set needs_revoking = {} %}
            {% set needs_granting = grant_config %}
        {% endif %}
        {% if needs_granting or needs_revoking %}
            {% set revoke_statement_list = get_revoke_sql(relation, needs_revoking) %}
            {% set grant_statement_list = get_grant_sql(relation, needs_granting) %}
            {% set grant_and_revoke_statement_list = revoke_statement_list + grant_statement_list %}
            {% if grant_and_revoke_statement_list %}
                {{ call_grant_revoke_statement_list(grant_and_revoke_statement_list) }}
            {% endif %}
        {% endif %}
    {% endif %}
{% endmacro %}
