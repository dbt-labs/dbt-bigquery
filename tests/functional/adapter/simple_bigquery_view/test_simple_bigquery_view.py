import pytest
import random
import time
from dbt.tests.util import run_dbt
from dbt.tests.adapter.simple_seed.test_seed import SeedConfigBase
from tests.functional.adapter.simple_bigquery_view.seeds import *
from tests.functional.adapter.simple_bigquery_view.fixtures import *


class BaseBigQueryRun(SeedConfigBase):
    @pytest.fixture(scope="class")
    def schema(self):
        return "bigquery_test"

    @pytest.fixture(scope="class")
    def macros(self):
        return {
            "test_creation.sql": test_creation_sql,
            "test_int_inference.sql": test_int_inference_sql,
            "test_project_for_job_id.sql": test_project_for_job_id_sql,
            "wrapped_macros.sql": wrapped_macros_sql
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "clustered_model.sql": clustered_model_sql,
            "fUnKyCaSe.sql": funky_case_sql,
            "labeled_model.sql": labeled_model_sql,
            "multi_clustered_model.sql": multi_clustered_model_sql,
            "partitioned_model.sql": partitioned_model_sql,
            "sql_header_model.sql": sql_header_model_sql,
            "sql_header_model_incr.sql": sql_header_model_incr_sql,
            "sql_header_model_incr_insert_overwrite.sql": sql_header_model_incr_insert_overwrite_sql,
            "sql_header_model_incr_insert_overwrite_static.sql": sql_header_model_incr_insert_overwrite_static_sql,
            "table_model.sql": tabel_model_sql,
            "view_model.sql": view_model_sql,
            "schema.yml": schema_yml
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

    def assert_nondupes_pass(self, project):
        # The 'dupe' model should fail, but all others should pass
        test_results = run_dbt(['test'], expect_pass=False)

        for test_result in test_results:
            if 'dupe' in test_result.node.name:
                assert test_result.status == 'fail'
                assert not test_result.skipped
                assert test_result.failures > 0

            # assert that actual tests pass
            else:
                assert test_result.status == 'pass'
                assert not test_result.skipped
                assert test_result.failures == 0


class TestSimpleBigQueryRun(BaseBigQueryRun):
    def test__bigquery_simple_run(self, project):
        # make sure seed works twice. Full-refresh is a no-op
        run_dbt(['seed'])
        run_dbt(['seed', '--full-refresh'])

        results = run_dbt()
        # Bump expected number of results when adding new model
        assert len(results) == 11
        self.assert_nondupes_pass(project)


class TestUnderscoreBigQueryRun(BaseBigQueryRun):
    prefix = "_test{}{:04}".format(int(time.time()), random.randint(0, 9999))

    def test_bigquery_run_twice(self, project):
        run_dbt(['seed'])
        results = run_dbt()
        assert len(results) == 11

        results = run_dbt()
        assert len(results) == 11

        self.assert_nondupes_pass(project)
