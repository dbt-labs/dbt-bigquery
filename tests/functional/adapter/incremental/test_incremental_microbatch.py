import os
import pytest
from unittest import mock

from dbt.tests.util import run_dbt_and_capture
from dbt.tests.adapter.incremental.test_incremental_microbatch import (
    BaseMicrobatch,
    patch_microbatch_end_time,
)

from tests.functional.adapter.incremental.incremental_strategy_fixtures import (
    microbatch_model_no_unique_id_sql,
    microbatch_input_sql,
    microbatch_model_no_partition_by_sql,
    microbatch_model_invalid_partition_by_sql,
)


class TestBigQueryMicrobatch(BaseMicrobatch):
    @pytest.fixture(scope="class")
    def microbatch_model_sql(self) -> str:
        return microbatch_model_no_unique_id_sql


class TestBigQueryMicrobatchMissingPartitionBy:
    @pytest.fixture(scope="class")
    def models(self) -> str:
        return {
            "microbatch.sql": microbatch_model_no_partition_by_sql,
            "input_model.sql": microbatch_input_sql,
        }

    def test_execution_failure_no_partition_by(self, project):
        with patch_microbatch_end_time("2020-01-03 13:57:00"):
            _, stdout = run_dbt_and_capture(["run"], expect_pass=False)
        assert "The 'microbatch' strategy requires a `partition_by` config" in stdout


class TestBigQueryMicrobatchInvalidPartitionByGranularity:
    @pytest.fixture(scope="class")
    def models(self) -> str:
        return {
            "microbatch.sql": microbatch_model_invalid_partition_by_sql,
            "input_model.sql": microbatch_input_sql,
        }

    def test_execution_failure_no_partition_by(self, project):
        with patch_microbatch_end_time("2020-01-03 13:57:00"):
            _, stdout = run_dbt_and_capture(["run"], expect_pass=False)
        assert (
            "The 'microbatch' strategy requires a `partition_by` config with the same granularity as its configured `batch_size`"
            in stdout
        )
