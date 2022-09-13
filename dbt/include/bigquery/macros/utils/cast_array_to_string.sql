{% macro bigquery__cast_array_to_string(array) %}
    '['||(select string_agg(cast(element as string), ',') from unnest({{ array }}) element)||']'
{% endmacro %}
