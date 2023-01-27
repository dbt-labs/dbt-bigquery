import pytest
import os
from dbt.tests.util import run_dbt_and_capture

_MODEL_SQL = """
select 1 as id
"""

_INVALID_LOCATION = os.getenv('DBT_TEST_BIGQUERY_BAD_LOCATION', 'northamerica-northeast1')

class BaseBigQueryLocation:

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "model.sql": _MODEL_SQL,
        }


class TestBigqueryValidLocation(BaseBigQueryLocation):

    def test_bigquery_valid_location(self, project):
        _, stdout = run_dbt_and_capture()
        assert "Completed successfully" in stdout



class TestBigqueryInvalidLocation(BaseBigQueryLocation):

    @pytest.fixture(scope="class")
    def profiles_config_update(self, dbt_profile_target):
        outputs = {"default": dbt_profile_target}
        outputs["default"]["location"] = _INVALID_LOCATION
        return {"test": {"outputs": outputs, "target": "default"}}

    def test_bigquery_location_invalid(self, project):
        _, stdout = run_dbt_and_capture()
        assert "Query Job SQL Follows"  not in stdout
