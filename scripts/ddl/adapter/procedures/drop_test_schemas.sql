/*
This procedure will drop every test schema, as defined by `adapter.test_schema`, for which it has access.
In the event this procedure does not have access, it sill simply print out the name of the schema and continue.
*/
CREATE OR REPLACE PROCEDURE adapter.drop_test_schemas()
BEGIN
    FOR test_schema IN (SELECT * FROM adapter.test_schema)
    DO
        BEGIN
            EXECUTE IMMEDIATE CONCAT("DROP SCHEMA IF EXISTS `", test_schema.schema_name, "` CASCADE");
        EXCEPTION WHEN ERROR THEN
            SELECT CONCAT("Encountered an error when dropping `", test_schema.schema_name, "`. Skipping.");
        END;
    END FOR;
END;
