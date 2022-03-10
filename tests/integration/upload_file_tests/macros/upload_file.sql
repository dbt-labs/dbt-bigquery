{% macro upload_file(local_file_path, database, table_schema, table_name) %}
  {% do adapter.upload_file(local_file_path, database, table_schema, table_name, kwargs=kwargs) %}
{% endmacro %}