{% macro bigquery__drop_relation(relation) -%}
  {% call statement('drop_relation') -%}
    drop {{ relation.type }} if exists {{ relation }}
  {%- endcall %}
{% endmacro %}


{% macro bigquery__drop_relation_sql(relation) -%}
  {% if relation.type == "materialized_view" %}
    drop_materialized_view_sql(relation)
  {% else %}
    drop {{ relation.type }} if exists {{ relation }} cascade
  {% endif %}
{% endmacro %}
