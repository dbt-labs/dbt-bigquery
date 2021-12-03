## dbt-bigquery 1.0.0 (December 3, 2021)

## dbt-bigquery 1.0.0rc2 (November 24, 2021)

### Features
- Add optional `scopes` profile configuration argument to reduce the BigQuery OAuth scopes down to the minimal set needed. ([#23](https://github.com/dbt-labs/dbt-bigquery/issues/23), [#63](https://github.com/dbt-labs/dbt-bigquery/pull/63))

### Fixes
- Don't apply `require_partition_filter` to temporary tables, thereby fixing `insert_overwrite` strategy when partition filter is required ([#64](https://github.com/dbt-labs/dbt-bigquery/issues/64)), ([#65](https://github.com/dbt-labs/dbt-bigquery/pull/65))

### Under the hood
- Adding `execution_project` to `target` object ([#66](https://github.com/dbt-labs/dbt-bigquery/issues/66))

### Contributors
- [@pgoslatara](https://github.com/pgoslatara) ([#66](https://github.com/dbt-labs/dbt-bigquery/issues/66))
- [@bborysenko](https://github.com/bborysenko) ([#63](https://github.com/dbt-labs/dbt-bigquery/pull/63))
- [@yu-iskw](https://github.com/yu-iskw) ([#65](https://github.com/dbt-labs/dbt-bigquery/pull/65))

## dbt-bigquery 1.0.0rc1 (November 10, 2021)

### Fixes
- Fix problem with bytes processed return None value when the service account used to connect DBT in bigquery had a row policy access.
([#47](https://github.com/dbt-labs/dbt-bigquery/issues/47), [#48](https://github.com/dbt-labs/dbt-bigquery/pull/48))
- When on_schema_change is set, pass common columns as dest_columns in incremental merge macros ([#4144](https://github.com/dbt-labs/dbt-core/issues/4144))

### Under the hood
- Capping `google-api-core` to version `1.31.3` due to `protobuf` dependency conflict ([#53](https://github.com/dbt-labs/dbt-bigquery/pull/53))
- Bump `google-cloud-core` and `google-api-core` upper bounds to `<3`, thereby removing `<1.31.3` limit on the latter. Remove explicit dependency on `six` ([#57](https://github.com/dbt-labs/dbt-bigquery/pull/57))
- Remove official support for python 3.6, which is reaching end of life on December 23, 2021 ([dbt-core#4134](https://github.com/dbt-labs/dbt-core/issues/4134), [#59](https://github.com/dbt-labs/dbt-bigquery/pull/59))
- Add support for structured logging [#55](https://github.com/dbt-labs/dbt-bigquery/pull/55)

### Contributors
- [@imartynetz](https://github.com/imartynetz) ([#48](https://github.com/dbt-labs/dbt-bigquery/pull/48))
- [@Kayrnt](https://github.com/Kayrnt) ([#51](https://github.com/dbt-labs/dbt-bigquery/pull/51))

## dbt-bigquery 1.0.0b2 (October 25, 2021)

### Features

- Rework `_dbt_max_partition` logic in dynamic `insert_overwrite` incremental strategy. Make the default logic compatible with `on_schema_change`, and make it possible to disable or reimplement that logic by defining a custom macro `declare_dbt_max_partition` ([#17](https://github.com/dbt-labs/dbt-bigquery/issues/17), [#39](https://github.com/dbt-labs/dbt-bigquery/issues/39), [#41](https://github.com/dbt-labs/dbt-bigquery/pull/41))

### Fixes
- Reimplement the `unique` test to handle column expressions and naming overlaps ([#33](https://github.com/dbt-labs/dbt-bigquery/issues/33), [#35](https://github.com/dbt-labs/dbt-bigquery/issues/35), [#10](https://github.com/dbt-labs/dbt-bigquery/pull/10))
- Avoid error in `dbt deps` + `dbt clean` if default project is missing ([#27](https://github.com/dbt-labs/dbt-bigquery/issues/27), [#40](https://github.com/dbt-labs/dbt-bigquery/pull/40))

### Under the hood
- Replace `sample_profiles.yml` with `profile_template.yml`, for use with new `dbt init` ([#43](https://github.com/dbt-labs/dbt-bigquery/pull/43))

### Contributors

- [@DigUpTheHatchet](https://github.com/DigUpTheHatchet) ([#10](https://github.com/dbt-labs/dbt-bigquery/pull/10))
- [@jeremyyeo](https://github.com/jeremyyeo) ([#40](https://github.com/dbt-labs/dbt-bigquery/pull/40))
- [@NiallRees](https://github.com/NiallRees) ([#43](https://github.com/dbt-labs/dbt-bigquery/pull/43))

## dbt-bigquery 1.0.0b1 (October 11, 2021)

### Under the hood
- Initial adapter split out
