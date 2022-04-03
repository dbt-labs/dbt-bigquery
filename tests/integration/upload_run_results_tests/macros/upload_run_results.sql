{% macro upload_run_results(local_file_path, database, table_schema, table_name) %}
  {% do adapter.upload_run_results(local_file_path, database, table_schema, table_name, kwargs=kwargs) %}
{% endmacro %}