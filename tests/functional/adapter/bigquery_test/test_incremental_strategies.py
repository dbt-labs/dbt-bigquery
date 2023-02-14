import pytest
from dbt.tests.util import (
    check_relations_equal,
    run_dbt
)
from dbt.tests.adapter.simple_seed.test_seed import SeedConfigBase
from tests.functional.adapter.bigquery_test.seeds import *
from tests.functional.adapter.bigquery_test.incremental_strategy_fixtures import *


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
        }

    @pytest.fixture(scope="class")
    def seeds(self):
        return {
            "data_seed.csv": seed_data_csv,
            "merge_expected.csv": seed_merge_expected_csv,
            "incremental_overwrite_time_expected.csv": seed_incremental_overwrite_time_expected_csv,
            "incremental_overwrite_date_expected.csv": seed_incremental_overwrite_date_expected_csv,
            "incremental_overwrite_day_expected.csv": seed_incremental_overwrite_day_expected_csv,
            "incremental_overwrite_range_expected.csv": seed_incremental_overwrite_range_expected_csv
        }

    def test__bigquery_assert_incrementals(self, project):
        results = run_dbt()
        assert len(results) == 8

        results = run_dbt()
        assert len(results) == 8

        seed_results = run_dbt(['seed'])

        db_with_schema = f"{project.database}.{project.test_schema}"
        check_relations_equal(project.adapter, [f"{db_with_schema}.incremental_merge_range",
                                                f"{db_with_schema}.merge_expected"])
        check_relations_equal(project.adapter, [f"{db_with_schema}.incremental_merge_time",
                                                f"{db_with_schema}.merge_expected"])
        check_relations_equal(project.adapter, [f"{db_with_schema}.incremental_overwrite_time",
                                                f"{db_with_schema}.incremental_overwrite_time_expected"])
        check_relations_equal(project.adapter, [f"{db_with_schema}.incremental_overwrite_date",
                                                f"{db_with_schema}.incremental_overwrite_date_expected"])
        check_relations_equal(project.adapter, [f"{db_with_schema}.incremental_overwrite_partitions",
                                                f"{db_with_schema}.incremental_overwrite_date_expected"])
        check_relations_equal(project.adapter, [f"{db_with_schema}.incremental_overwrite_day",
                                                f"{db_with_schema}.incremental_overwrite_day_expected"])
        check_relations_equal(project.adapter, [f"{db_with_schema}.incremental_overwrite_range",
                                                f"{db_with_schema}.incremental_overwrite_range_expected"])
        check_relations_equal(project.adapter, [f"{db_with_schema}.incremental_overwrite_day_with_copy_partitions",
                                                f"{db_with_schema}.incremental_overwrite_day_expected"])

