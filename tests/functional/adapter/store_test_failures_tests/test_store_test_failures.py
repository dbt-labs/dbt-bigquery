import pytest

from dbt.tests.adapter.store_test_failures_tests import basic
from dbt.tests.adapter.store_test_failures_tests.test_store_test_failures import (
    StoreTestFailuresBase,
)


TEST_AUDIT_SCHEMA_SUFFIX = "dbt_test__aud"


class TestBigQueryStoreTestFailures(StoreTestFailuresBase):
    @pytest.fixture(scope="function", autouse=True)
    def teardown_method(self, project):
        yield
        relation = project.adapter.Relation.create(
            database=project.database, schema=f"{project.test_schema}_{TEST_AUDIT_SCHEMA_SUFFIX}"
        )

        project.adapter.drop_schema(relation)

    def test_store_and_assert(self, project):
        self.run_tests_store_one_failure(project)
        self.run_tests_store_failures_and_assert(project)


class TestStoreTestFailuresAsInteractions(basic.StoreTestFailuresAsInteractions):
    pass


class TestStoreTestFailuresAsProjectLevelOff(basic.StoreTestFailuresAsProjectLevelOff):
    pass


class TestStoreTestFailuresAsProjectLevelView(basic.StoreTestFailuresAsProjectLevelView):
    pass


class TestStoreTestFailuresAsGeneric(basic.StoreTestFailuresAsGeneric):
    pass


class TestStoreTestFailuresAsProjectLevelEphemeral(basic.StoreTestFailuresAsProjectLevelEphemeral):
    pass


class TestStoreTestFailuresAsExceptions(basic.StoreTestFailuresAsExceptions):
    pass
