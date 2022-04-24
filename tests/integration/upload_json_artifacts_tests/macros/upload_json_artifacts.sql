{% macro upload_json_artifacts(local_file_path, database, table_schema, replacement_string="__") %}
  {% do adapter.upload_json_artifacts(local_file_path, database, table_schema, replacement_string, kwargs=kwargs) %}
{% endmacro %}