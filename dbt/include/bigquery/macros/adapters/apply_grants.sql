{% macro  bigquery__get_show_grant_sql(relation) %}
    {% if not target.location %}
        {{ exceptions.raise_compiler_error("In order to use the grants feature, you must specify a location ") }}
    {% endif %}

    select privilege_type, grantee from {{ relation.project }}.`region-{{ target.location }}`.INFORMATION_SCHEMA.OBJECT_PRIVILEGES
    where object_schema = "{{ relation.dataset }}" and object_name = "{{ relation.identifier }}"
{% endmacro %}


{%- macro bigquery__get_grant_sql(relation, grant_config) -%}
    {%- for privilege in grant_config.keys() -%}
        {%- set grantees = grant_config[privilege] -%}
        {%- if grantees -%}
            {%- for grantee in grantees -%}
                grant `{{ privilege }}` on {{ relation.type }} {{ relation }} to "{{ grantee }}";
            {%- endfor -%}
        {%- endif -%}
    {%- endfor -%}
{%- endmacro %}


{% macro bigquery__get_revoke_sql(relation, grant_config) %}
    {%- for privilege in grant_config.keys() -%}
        {%- set grantees = [] -%}
        {%- set all_grantees = grant_config[privilege] -%}
        {%- for grantee in all_grantees -%}
            {%- if grantee != target.user -%}
                {% do grantees.append(grantee) %}
            {%- endif -%}
        {%- endfor -%}
        {%- if grantees -%}
            {%- for grantee in grantees -%}
                revoke `{{ privilege }}` on {{ relation.type }} {{ relation }}  from "{{ grantee }}";
            {% endfor -%}
        {%- endif -%}
    {%- endfor -%}
{%- endmacro -%}
