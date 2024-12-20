# array_append

# EXCEPT can't be used with ARRAYs in BigQuery, so convert to a string
models__array_append_expected_sql = """
select 1 as id, {{ array_to_string(array_construct([1,2,3,4])) }} as array_col union all
select 2 as id, {{ array_to_string(array_construct([4])) }} as array_col
"""


models__array_append_actual_sql = """
select 1 as id, {{ array_to_string(array_append(array_construct([1,2,3]), 4)) }} as array_col union all
select 2 as id, {{ array_to_string(array_append(array_construct([]), 4)) }} as array_col
"""
