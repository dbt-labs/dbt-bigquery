/*
This view groups test schemas by the integration test that created them.
The goal is to identify any integration tests which may not be properly tearing down their test schema.
*/
CREATE OR REPLACE VIEW adapter.test_schema_summary AS
    SELECT
        CASE
            WHEN LENGTH(schema_name) > 13
            THEN RIGHT(schema_name, LENGTH(schema_name)-LENGTH('test12345678901234567890_'))
            ELSE '13-digit strings'
        END AS integration_test,
        COUNT(schema_name) AS schema_count
    FROM adapter.test_schema
    GROUP BY 1
    ORDER BY 2 DESC
;
