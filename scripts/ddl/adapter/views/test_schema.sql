/*
This view identifies all test schemas that `dbt-bigquery` creates in the course of running integration tests.
Ideally this view will return no records as test schemas should be ephemeral in nature.
However, there are many scenarios where a test schema will be left behind. The two most common are:
- the fixture simply does not delete it when it's supposed to
- the connection is broken
*/
CREATE OR REPLACE VIEW adapter.test_schema AS
    SELECT *
    FROM `dbt-test-env`.INFORMATION_SCHEMA.SCHEMATA
    WHERE (
        schema_name LIKE 'test%_test_%' OR         -- e.g. test12345678901234567890_test_basic
        REGEXP_CONTAINS(schema_name, r'^\d{13}$')  -- e.g. 1234567890123
    )
    AND catalog_name = 'dbt-test-env'
;
