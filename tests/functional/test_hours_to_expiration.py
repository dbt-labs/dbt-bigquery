import pytest
from dbt.tests.util import run_dbt_and_capture

_MODEL_SQL = """
select 1 as id
"""


class BaseBigQueryHoursToExpiration:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "model.sql": _MODEL_SQL,
        }

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "models": {"test": {"materialized": "table", "model": {"hours_to_expiration": "4"}}}
        }


class TestBigQueryHoursToExpiration(BaseBigQueryHoursToExpiration):
    def test_bigquery_hours_to_expiration(self, project):
        _, stdout = run_dbt_and_capture(["--debug", "run"])
        assert "expiration_timestamp=TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 4 hour)" in stdout
