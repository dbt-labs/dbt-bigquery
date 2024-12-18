{% macro bigquery__rename_relation(from_relation, to_relation) -%}
  {% do adapter.rename_relation(from_relation, to_relation) %}
{% endmacro %}
