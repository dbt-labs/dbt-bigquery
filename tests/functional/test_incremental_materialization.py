import pytest
from dbt.tests.util import run_dbt

# This is a short term hack, we need to go back
# and make adapter implementations of:
# https://github.com/dbt-labs/dbt-core/pull/6330

_INCREMENTAL_MODEL = """
{{
    config(
        materialized="incremental",
    )
}}

{% if not is_incremental() %}

    select 10 as id, cast('2020-01-01 01:00:00' as datetime) as date_hour union all
    select 30 as id, cast('2020-01-01 02:00:00' as datetime) as date_hour

{% else %}

    select 20 as id, cast('2020-01-01 01:00:00' as datetime) as date_hour union all
    select 40 as id, cast('2020-01-01 02:00:00' as datetime) as date_hour

{% endif %}
-- Test Comment To Prevent Reccurence of https://github.com/dbt-labs/dbt-core/issues/6485
"""


class BaseIncrementalModelConfig:
    @pytest.fixture(scope="class")
    def models(self):
        return {"test_incremental.sql": _INCREMENTAL_MODEL}


class TestIncrementalModel(BaseIncrementalModelConfig):
    def test_incremental_model_succeeds(self, project):
        results = run_dbt(["run"])
        assert len(results) == 1
        results = run_dbt(["run"])
        assert len(results) == 1
