from abc import abstractmethod
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
BAD_CONFIG_TABLE_NAME = "bad_view"
BAD_CONFIG_TABLE = """
{{ config(
    materialized='view',
    grant_access_to=[
      {'project': 'dbt-test-env-alt', 'dataset': 'NonExistentDataset'},
    ]
) }}
SELECT 1 as one
"""

BAD_CONFIG_CHILD_TABLE = "SELECT 1 as one FROM {{ref('" + BAD_CONFIG_TABLE_NAME + "')}}"


class TestAccessGrantSucceeds:
    @pytest.fixture(scope="class")
    def models(self):
        return {"select_1.sql": SELECT_1, "select_1_table.sql": SELECT_1_TABLE}

    def test_grant_access_succeeds(self, project):
        # Need to run twice to validate idempotency
        results = run_dbt(["run"])
        assert len(results) == 2
        results = run_dbt(["run"])
        assert len(results) == 2


class TestAccessGrantFails:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "bad_config_table_child.sql": BAD_CONFIG_CHILD_TABLE,
            f"{BAD_CONFIG_TABLE_NAME}.sql": BAD_CONFIG_TABLE,
        }

    def test_grant_access_fails_without_running_child_table(self, project):
        # Need to run twice to validate idempotency
        results = run_dbt(["run"], expect_pass=False)
        assert results[0].status == "error"
        assert results[1].status == "skipped"
        assert results[0].message.startswith("404 GET https://bigquery.googleapis.com/")
