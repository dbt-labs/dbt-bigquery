{% macro  bigquery__get_show_grant_sql(relation) %}
  {# Note: This only works if the location is defined in the profile.  It is an optional field right now. #}

select * from {{ relation.project }}.{{ target.location }}.INFORMATION_SCHEMA.OBJECT_PRIVILEGES
where object_schema = "{{ relation.dataset }}" and object_name = "{{ relation.identifier }}"
{% endmacro %}