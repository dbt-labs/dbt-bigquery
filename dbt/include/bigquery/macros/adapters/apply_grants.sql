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


{%- macro bigquery__get_grant_sql(relation, privilege, grantees) -%}
    {{ bigquery__get_grant_sql__option_3(relation, privilege, grantees) }}
{%- endmacro -%}


{%- macro bigquery__get_revoke_sql(relation, privilege, grantees) -%}
    {{ bigquery__get_revoke_sql__option_3(relation, privilege, grantees) }}
{%- endmacro -%}


{%- macro bigquery__get_grant_sql__option_X(relation, privilege, grantee) -%}
    grant {{ privilege }} on {{ relation }} to {{ grantees | join(', ') }}
{%- endmacro -%}


{%- macro bigquery__get_revoke_sql__option_X(relation, privilege, grantee) -%}
    revoke {{ privilege }} on {{ relation }} from {{ grantees | join(', ') }}
{%- endmacro -%}


{%- macro bigquery__get_grant_sql__option_0(relation, privilege, grantee) -%}
    grant `{{ privilege }}` on {{ relation.type }} {{ relation }} to {{ '\"' + grantee|join('\", \"') + '\"' }}
{%- endmacro -%}


{%- macro bigquery__get_revoke_sql__option_0(relation, privilege, grantee) -%}
    revoke `{{ privilege }}` on {{ relation.type }} {{ relation }} from {{ '\"' + grantee|join('\", \"') + '\"' }}
{%- endmacro -%}


{%- macro bigquery__get_grant_sql__option_1(relation, privilege, grantees) -%}
    grant {{ adapter.quote(privilege) }} on {{ relation.type }} {{ relation }} to {{ string_literal_csv(grantees) }}
{%- endmacro -%}


{%- macro bigquery__get_revoke_sql__option_1(relation, privilege, grantees) -%}
    revoke {{ adapter.quote(privilege) }} on {{ relation.type }} {{ relation }} from {{ string_literal_csv(grantees) }}
{%- endmacro -%}


{%- macro string_literal_csv(items) -%}
    {%- for item in items -%}
        {{ string_literal(item) }}{% if not loop.last %}, {% endif %}
    {% endfor %}
{%- endmacro -%}


{%- macro bigquery__get_grant_sql__option_2(relation, privilege, grantees) -%}
    grant {{ adapter.quote(privilege) }} on {{ relation.type }} {{ relation }} to {{ apply_string_literal(grantees) | join(', ') }}
{%- endmacro -%}


{%- macro bigquery__get_revoke_sql__option_2(relation, privilege, grantees) -%}
    revoke {{ adapter.quote(privilege) }} on {{ relation.type }} {{ relation }} from {{ apply_string_literal(grantees) | join(', ') }}
{%- endmacro -%}


{%- macro apply_string_literal(items) -%}
    {%- set apply_items = [] -%}
    {%- for item in items -%}
        {{ apply_items.append(string_literal(item)) }}
    {% endfor %}
    {%- do return(apply_items) -%}
{%- endmacro -%}


{%- macro bigquery__get_grant_sql__option_3(relation, privilege, grantees) -%}
    grant {{ adapter.quote(privilege) }} on {{ relation.type }} {{ relation }} to {{ apply(string_literal, grantees) | join(', ') }}
{%- endmacro -%}


{%- macro bigquery__get_revoke_sql__option_3(relation, privilege, grantees) -%}
    revoke {{ adapter.quote(privilege) }} on {{ relation.type }} {{ relation }} from {{ apply(string_literal, grantees) | join(', ') }}
{%- endmacro -%}


{%- macro apply(macro, items) -%}
    {%- set apply_items = [] -%}
    {%- for item in items -%}
        {{ apply_items.append(macro(item)) }}
    {% endfor %}
    {%- do return(apply_items) -%}
{%- endmacro -%}
