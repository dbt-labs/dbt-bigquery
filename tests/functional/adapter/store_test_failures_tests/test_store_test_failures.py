import pytest

from dbt.tests.adapter.store_test_failures_tests.test_store_test_failures import StoreTestFailuresBase


class TestBigQueryStoreTestFailures(StoreTestFailuresBase):
    def test_store_and_assert(self, project):
        self.run_tests_store_one_failure(project)
        self.run_tests_store_failures_and_assert(project)
