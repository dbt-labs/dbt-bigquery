import pytest
import os
from dbt.tests.util import run_dbt

_MODEL_SQL = """
select 1 as id
"""

_INVALID_LOCATION = os.getenv("DBT_TEST_BIGQUERY_BAD_LOCATION", "northamerica-northeast1")
_VALID_LOCATION = os.getenv("DBT_TEST_BIGQUERY_INITIAL_LOCATION", "US")


class BaseBigQueryLocation:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "model.sql": _MODEL_SQL,
        }


class TestBigqueryValidLocation(BaseBigQueryLocation):
    def test_bigquery_valid_location(self, project):
        results = run_dbt()
        for result in results:
            assert "US" == result.adapter_response["location"]


class TestBigqueryInvalidLocation(BaseBigQueryLocation):
    @pytest.fixture(scope="class")
    def profiles_config_update(self, dbt_profile_target):
        outputs = {"default": dbt_profile_target}
        outputs["default"]["location"] = _INVALID_LOCATION
        yield
        outputs = {"default": dbt_profile_target}
        outputs["default"]["location"] = _VALID_LOCATION

    def test_bigquery_location_invalid(self, project):
        results = run_dbt()
        for result in results:
            assert "northamerica-northeast1" == result.adapter_response["location"]
