import pytest

from dbt.tests.util import (
    check_relations_equal,
    run_dbt
)

from tests.functional.adapter.bigquery_test.incremental_strategy_fixtures import overwrite_day_with_time_ingestion_sql
from tests.functional.adapter.bigquery_test.seeds import seed_incremental_overwrite_day_with_time_partition_expected_csv


class TestCustomBigQueryIncrementalStrategies:
    @pytest.fixture(scope="class")
    def models(self):
        return {"incremental_overwrite_day_with_time_ingestion.sql": overwrite_day_with_time_ingestion_sql}

    @pytest.fixture(scope="class")
    def seeds(self):
        return {
            "incremental_overwrite_day_with_time_partition_expected.csv": seed_incremental_overwrite_day_with_time_partition_expected_csv
        }

    def test_valid_incremental_strategies_succeed(self, project):
        results = run_dbt()
        assert len(results) == 1
        results = run_dbt()