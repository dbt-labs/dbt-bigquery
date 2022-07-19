{% macro bigquery__safe_cast(field, type) %}
    safe_cast({{field}} as {{type}})
{% endmacro %}
