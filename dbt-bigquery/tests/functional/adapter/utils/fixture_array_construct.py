# array_construct

# EXCEPT can't be used with ARRAYs in BigQuery, so convert to a string
models__array_construct_expected_sql = """
select 1 as id, {{ array_to_string(array_construct([1,2,3])) }} as array_col union all
select 2 as id, {{ array_to_string(array_construct([])) }} as array_col
"""


models__array_construct_actual_sql = """
select 1 as id, {{ array_to_string(array_construct([1,2,3])) }} as array_col union all
select 2 as id, {{ array_to_string(array_construct([])) }} as array_col
"""


macros__array_to_string_sql = """
{% macro array_to_string(array) %}
    (select string_agg(cast(element as string), ',') from unnest({{ array }}) element)
{% endmacro %}
"""
