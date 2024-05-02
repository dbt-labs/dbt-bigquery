# Structure of the CI database (`dbt-test-env`)

## `adapter`

### Objects

- [schema](adapter/schema.sql)
- views
  - [test_schema](adapter/views/test_schema.sql)
  - [test_schema_summary](adapter/views/test_schema_summary.sql)
- procedures
  - [drop_test_schemas](adapter/procedures/drop_test_schemas.sql)

> Please refer to each DDL file, linked for convenience, for details about the particular object

### Description

The `adapter` schema contains objects that facilitate the maintenance of the test environment.
The following tasks can be accomplished within this environment:
- identify orphan test schemas
- identify integration tests that are creating orphan schemas
- drop orphan schemas

## `test%_test_%`

These schemas are temporary schemas created in the course of integration testing.
They are of the format `test<unique-hash>_<test-item>`, e.g. `test12345678901234567890_test_basic`.
The `unique-hash` part is a combination of a timestamp and a random four-digit integer.
The `test-item` part is either a test module (usually), test class, or test function.

## `dbt_<user>`

These schemas are user workspaces that allow for development and troubleshooting.
Each schema is up to the user for maintaining and there are no scripts persisted in this repo.

## Additional schemas out of scope of this application

- `always_sunny_%`: These schemas are used by `always-sunny`, and contain persistent objects
- `dbt_cloud_pr_%`: Used by Cloud's CI process?
- `us_q_team_pristine_prod_%`: Used by Quality Team's Pristine setups?
- `dbt_us_bigquery_%`
- `semantic_layer_%`
