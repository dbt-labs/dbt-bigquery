models__bq_test_get_intervals_between_sql = """
SELECT
  {{ get_intervals_between("'2023-09-01'", "'2023-09-12'", "day") }} as intervals,
  11 as expected

"""

models___bq_test_get_intervals_between_yml = """
version: 2
models:
  - name: test_get_intervals_between
    tests:
      - assert_equal:
          actual: intervals
          expected: expected
"""
