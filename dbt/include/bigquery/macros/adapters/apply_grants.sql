{% macro  bigquery__get_show_grant_sql(relation) %}
{# Note: This only works if the location is defined in the profile.  It is an optional field right now. #}
{# TODO: location is hardcoded for now - need logic around this (default to us)#}
select * from {{ relation.project }}.`region-us`.INFORMATION_SCHEMA.OBJECT_PRIVILEGES
where object_schema = "{{ relation.dataset }}" and object_name = "{{ relation.identifier }}"
{% endmacro %}

{% macro bigquery__get_grant_sql(relation, grant_config) %}
    {% for privilege in grant_config.keys() %}
     {{ log('privilege: ' ~ privilege) }}
        {% set grantees = grant_config[privilege] %}
        {% for grantee in grantees %}
            grant `{{ privilege }}` on {{ relation.type }} {{ relation.dataset }}.{{ relation.identifier }} to "{{ grantee }}";
        {% endfor %}
    {% endfor %}
{% endmacro %}

{% macro bigquery__get_revoke_sql(relation, grant_config) %}
{# TODO: will probably need to do some more tweaks around revoke and table fields once that is resolved in core #}
    {% for privilege in grant_config.keys() %}
        {% set grantees = grant_config[privilege] %}
        {% for grantee in grantees if grantee !=  target.user %}
            revoke `{{ privilege }}` on {{ relation.type }} {{ relation.dataset }}.{{ relation.identifier }} from "{{ grantee }}";
        {% endfor %}
    {% endfor %}
{% endmacro %}

