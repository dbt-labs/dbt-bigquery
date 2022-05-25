
{% macro bigquery__create_csv_table(model, agate_table) %}
    -- no-op
{% endmacro %}

{% macro bigquery__reset_csv_table(model, full_refresh, old_relation, agate_table) %}
    {{ adapter.drop_relation(old_relation) }}
{% endmacro %}

{% macro bigquery__load_csv_rows(model, agate_table) %}

  {%- set column_override = model['config'].get('column_types', {}) -%}
  {%- set kms_key_name = model['config'].get('kms_key_name', {}) -%}
  {{ adapter.load_dataframe(model['database'], model['schema'], model['alias'],
  							agate_table, column_override, kms_key_name) }}
  {% if config.persist_relation_docs() and 'description' in model %}

  	{{ adapter.update_table_description(model['database'], model['schema'], model['alias'], model['description']) }}
  {% endif %}
{% endmacro %}
