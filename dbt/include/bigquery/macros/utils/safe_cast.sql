{% macro bigquery__safe_cast(field, type) %}
{%- if type.lower().startswith('array') and field is iterable and (field is not string and field is not mapping) and field | length > 0 -%}
    (select array_agg(safe_cast(i as {{type.lower()[6:-1]}})) from unnest({{field}}) i)
{%- elif type.lower() == 'json' and field is mapping -%}
    safe_cast(json {{ dbt.string_literal(tojson(field)) }} as json)
{%- else -%}
    safe_cast({{field}} as {{type}})
{%- endif -%}
{% endmacro %}
