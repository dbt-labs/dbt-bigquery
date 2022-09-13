{% macro bigquery__array_construct(inputs, data_type) -%}
    [ {{ inputs|join(' , ') }} ]
{%- endmacro %}
