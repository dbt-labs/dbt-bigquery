{% macro  bigquery__get_show_grant_sql(relation) %}
  select * from {{ relation.project }}.'{{ target.location }}'.INFORMATION_SCHEMA.OBJECT_PRIVILEGES
  where object_schema = '{{ relation.dataset }}' and object_name = '{{ relation.identifier }}'
{% endmacro %}