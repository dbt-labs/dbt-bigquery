{% macro bigquery__get_show_grant_sql(relation) %}
    {% if not target.location %}
        {{ exceptions.raise_compiler_error("In order to use the grants feature, you must specify a location ") }}
    {% endif %}

    select privilege_type, grantee
    from `{{ relation.project }}`.`region-{{ target.location }}`.INFORMATION_SCHEMA.OBJECT_PRIVILEGES
    where object_schema = "{{ relation.dataset }}"
      and object_name = "{{ relation.identifier }}"
      -- filter out current user
      and split(grantee, ':')[offset(1)] != session_user()
{% endmacro %}


{%- macro bigquery__get_grant_sql(relation, privilege, grantee) -%}
    grant `{{ privilege }}` on {{ relation.type }} {{ relation }} to {{ '\"' + grantee|join('\", \"') + '\"' }}
{%- endmacro -%}

{%- macro bigquery__get_revoke_sql(relation, privilege, grantee) -%}
    revoke `{{ privilege }}` on {{ relation.type }} {{ relation }} from {{ '\"' + grantee|join('\", \"') + '\"' }}
{%- endmacro -%}
