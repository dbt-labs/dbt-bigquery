import pytest
from dbt.tests.util import (
    check_relations_equal,
    get_relation_columns,
    run_dbt,
)
from dbt.tests.adapter.simple_seed.test_seed import SeedConfigBase
from tests.functional.adapter.incremental.seeds import *
from tests.functional.adapter.incremental.incremental_strategy_fixtures import *


class TestBigQueryScripting(SeedConfigBase):
    @pytest.fixture(scope="class")
    def schema(self):
        return "bigquery_test"

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "incremental_merge_range.sql": merge_range_sql,
            "incremental_merge_time.sql": merge_time_sql,
            "incremental_overwrite_date.sql": overwrite_date_sql,
            "incremental_overwrite_day.sql": overwrite_day_sql,
            "incremental_overwrite_day_with_copy_partitions.sql": overwrite_day_with_copy_partitions_sql,
            "incremental_overwrite_partitions.sql": overwrite_partitions_sql,
            "incremental_overwrite_range.sql": overwrite_range_sql,
            "incremental_overwrite_time.sql": overwrite_time_sql,
            "incremental_overwrite_day_with_time_ingestion.sql": overwrite_day_with_time_ingestion_sql
        }

    @pytest.fixture(scope="class")
    def seeds(self):
        return {
            "data_seed.csv": seed_data_csv,
            "merge_expected.csv": seed_merge_expected_csv,
            "incremental_overwrite_time_expected.csv": seed_incremental_overwrite_time_expected_csv,
            "incremental_overwrite_date_expected.csv": seed_incremental_overwrite_date_expected_csv,
            "incremental_overwrite_day_expected.csv": seed_incremental_overwrite_day_expected_csv,
            "incremental_overwrite_range_expected.csv": seed_incremental_overwrite_range_expected_csv,
            "incremental_overwrite_day_with_time_partition_expected.csv": seed_incremental_overwrite_day_with_time_partition_expected_csv
        }

    def test__bigquery_assert_incremental_configurations_apply_the_right_strategy(self, project):
        run_dbt(['seed'])
        results = run_dbt()
        assert len(results) == 9

        results = run_dbt()
        assert len(results) == 9
        incremental_strategies = [
            ('incremental_merge_range', 'merge_expected'),
            ("incremental_merge_time", "merge_expected"),
            ("incremental_overwrite_time",
             "incremental_overwrite_time_expected"),
            ("incremental_overwrite_date",
             "incremental_overwrite_date_expected"),
            ("incremental_overwrite_partitions",
             "incremental_overwrite_date_expected"),
            ("incremental_overwrite_day", "incremental_overwrite_day_expected"),
            ("incremental_overwrite_range", "incremental_overwrite_range_expected"),
        ]
        db_with_schema = f"{project.database}.{project.test_schema}"
        for incremental_strategy in incremental_strategies:
            created_table = f"{db_with_schema}.{incremental_strategy[0]}"
            expected_table = f"{db_with_schema}.{incremental_strategy[1]}"
            check_relations_equal(project.adapter, [created_table, expected_table])

        # since this table requires a partition filter which check_relations_equal doesn't support extra where clauses
        # we just check column types
        created = get_relation_columns(project.adapter, "incremental_overwrite_day_with_copy_partitions")
        expected = get_relation_columns(project.adapter, "incremental_overwrite_day_expected")
        assert created == expected
