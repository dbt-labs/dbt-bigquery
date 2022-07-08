{% macro  bigquery__get_show_grant_sql(relation) %}
    {% if not target.location %}
        {{ exceptions.raise_compiler_error("In order to use the grants feature, you must specify a location ") }}
    {% endif %}

    select privilege_type, grantee from {{ relation.project }}.`region-{{ target.location }}`.INFORMATION_SCHEMA.OBJECT_PRIVILEGES
    where object_schema = "{{ relation.dataset }}" and object_name = "{{ relation.identifier }}" and split(grantee, ':')[offset(1)] != session_user()
{% endmacro %}


{%- macro bigquery__get_grant_sql(relation, grant_config) -%}
    {%- for privilege in grant_config.keys() -%}
        {%- for grantee in grant_config[privilege] -%}
            grant `{{ privilege }}` on {{ relation.type }} {{ relation }} to "{{ grantee }}";
        {% endfor -%}
    {%- endfor -%}
{%- endmacro %}


{% macro bigquery__get_revoke_sql(relation, grant_config) %}
    {%- for privilege in grant_config.keys() -%}
        {%- for grantee in grant_config[privilege] -%}
            revoke `{{ privilege }}` on {{ relation.type }} {{ relation }} from "{{ grantee }}";
        {% endfor -%}
    {%- endfor -%}
{%- endmacro -%}
