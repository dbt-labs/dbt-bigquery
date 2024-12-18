{% macro bigquery__drop_relation(relation) -%}
    {% do adapter.drop_relation(relation) %}
{% endmacro %}
