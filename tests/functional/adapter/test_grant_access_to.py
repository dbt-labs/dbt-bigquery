import pytest
import os
from dbt.tests.util import run_dbt

SELECT_1 = """
{{ config(
    materialized='view',
    grant_access_to=[
      {'project': 'dbt-test-env-alt', 'dataset': 'GrantAccessTest'},
    ]
) }}
SELECT 1 as one
"""

SELECT_1_TABLE = """
{{ config(
    materialized='table',
    grant_access_to=[
      {'project': 'dbt-test-env-alt', 'dataset': 'GrantAccessTest'},
    ]
) }}
SELECT 1 as one
"""


class GrantAccessToConfig:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "select_1.sql": SELECT_1,
            "select_1_table.sql": SELECT_1_TABLE,
        }


class TestAccessGrantSucceeds(GrantAccessToConfig):
    def test_grant_access_succeeds(self, project):
        #Need to run twice to validate idempotency
        results = run_dbt(["run"])
        results = run_dbt(["run"])
        assert len(results) == 2
