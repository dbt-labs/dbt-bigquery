import pytest

from dbt.exceptions import DbtDatabaseError
from dbt.tests.util import run_dbt

_DEFAULT_TIMEOUT = 300
_SHORT_TIMEOUT = 1

_MODEL_SQL = """
    {{ config(job_execution_timeout_seconds=0.5, materialized='table') }}
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


class BaseJobTimeout:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "model.sql": _MODEL_SQL,
        }


class TestSuccessfulJobRun(BaseJobTimeout):
    def test_bigquery_default_job_run(self, project):
        run_dbt()


class TestJobTimeout(BaseJobTimeout):
    @pytest.fixture(scope="class")
    def profiles_config_update(self, dbt_profile_target):
        outputs = {"default": dbt_profile_target}
        outputs["default"]["job_execution_timeout_seconds"] = _SHORT_TIMEOUT
        return {"test": {"outputs": outputs, "target": "default"}}

    def test_job_timeout(self, project):
        with pytest.raises(DbtDatabaseError) as exc:
            run_dbt(["run"], expect_pass=False)  # project setup will fail
        assert f"Query exceeded configured timeout of {_SHORT_TIMEOUT}s" in str(exc.value)
