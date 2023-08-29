import pytest

from dbt.tests.util import run_dbt

_REASONABLE_TIMEOUT = 300
_SHORT_TIMEOUT = 1

_LONG_RUNNING_MODEL_SQL = """
    {{ config(materialized='table') }}
    with array_1 as (
    select generated_ids from UNNEST(GENERATE_ARRAY(1, 200000)) AS generated_ids
    ),
    array_2 as (
    select generated_ids from UNNEST(GENERATE_ARRAY(2, 200000)) AS generated_ids
    )

    SELECT array_1.generated_ids
    FROM array_1
    LEFT JOIN array_1 as jnd on 1=1
    LEFT JOIN array_2 as jnd2 on 1=1
    LEFT JOIN array_1 as jnd3 on jnd3.generated_ids >= jnd2.generated_ids
"""

_SHORT_RUNNING_QUERY = """
    SELECT 1 as id
    """


class TestSuccessfulJobRun:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "model.sql": _SHORT_RUNNING_QUERY,
        }

    @pytest.fixture(scope="class")
    def profiles_config_update(self, dbt_profile_target):
        outputs = {"default": dbt_profile_target}
        outputs["default"]["job_execution_timeout_seconds"] = _REASONABLE_TIMEOUT
        return {"test": {"outputs": outputs, "target": "default"}}

    def test_bigquery_job_run_succeeds_within_timeout(self, project):
        result = run_dbt()
        assert len(result) == 1


class TestJobTimeout:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "model.sql": _LONG_RUNNING_MODEL_SQL,
        }

    @pytest.fixture(scope="class")
    def profiles_config_update(self, dbt_profile_target):
        outputs = {"default": dbt_profile_target}
        outputs["default"]["job_execution_timeout_seconds"] = _SHORT_TIMEOUT
        return {"test": {"outputs": outputs, "target": "default"}}

    def test_job_timeout(self, project):
        result = run_dbt(["run"], expect_pass=False)  # project setup will fail
        assert f"Query exceeded configured timeout of {_SHORT_TIMEOUT}s" in result[0].message
