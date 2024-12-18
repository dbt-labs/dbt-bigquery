import os

import pytest

from dbt.tests.util import run_dbt

_QUOTA_PROJECT = os.getenv("BIGQUERY_TEST_ALT_DATABASE")


class TestNoQuotaProject:
    def test_no_quota_project(self, project):
        results = run_dbt()
        for result in results:
            assert None == result.adapter_response["quota_project"]


class TestQuotaProjectOption:
    @pytest.fixture(scope="class")
    def profiles_config_update(self, dbt_profile_target):
        outputs = {"default": dbt_profile_target}
        outputs["default"]["quota_project"] = _QUOTA_PROJECT
        yield

    def test_quota_project_option(self, project):
        results = run_dbt()
        for result in results:
            assert _QUOTA_PROJECT == result.adapter_response["quota_project"]
