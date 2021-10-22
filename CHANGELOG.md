## dbt-bigquery 1.0.0 (Release TBD)

### Features

- Rework `_dbt_max_partition` logic in dynamic `insert_overwrite` incremental strategy. Make the default logic compatible with `on_schema_change`, and make it possible to disable or reimplement that logic by defining a custom macro `declare_dbt_max_partition` ([#17](https://github.com/dbt-labs/dbt-bigquery/issues/17), [#39](https://github.com/dbt-labs/dbt-bigquery/issues/39), [#41](https://github.com/dbt-labs/dbt-bigquery/pull/41))

### Fixes

- Reimplement the `unique` test to handle column expressions and naming overlaps ([#33](https://github.com/dbt-labs/dbt-bigquery/issues/33), [#35](https://github.com/dbt-labs/dbt-bigquery/issues/35), [#10](https://github.com/dbt-labs/dbt-bigquery/pull/10))
- Avoid error in `dbt deps` + `dbt clean` if default project is missing ([#27](https://github.com/dbt-labs/dbt-bigquery/issues/27), [#40](https://github.com/dbt-labs/dbt-bigquery/pull/40))

### Contributors

- [@DigUpTheHatchet](https://github.com/DigUpTheHatchet) ([#10](https://github.com/dbt-labs/dbt-bigquery/pull/10))
- [@jeremyyeo](https://github.com/jeremyyeo) ([#40](https://github.com/dbt-labs/dbt-bigquery/pull/40))

## dbt-bigquery 1.0.0b1 (October 11, 2021)

### Under the hood
First version as a separate repo.
