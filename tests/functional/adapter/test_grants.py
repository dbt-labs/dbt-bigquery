import pytest

from dbt.tests.adapter.grants.base_grants import BaseGrants
from dbt.tests.adapter.grants.test_model_grants import BaseModelGrants
from dbt.tests.adapter.grants.test_incremental_grants import BaseIncrementalGrants
from dbt.tests.adapter.grants.test_invalid_grants import BaseInvalidGrants
from dbt.tests.adapter.grants.test_seed_grants import BaseSeedGrants
from dbt.tests.adapter.grants.test_snapshot_grants import BaseSnapshotGrants

from dbt.context.base import BaseContext  # diff_of_two_dicts_no_lower only


class BaseGrantsBigQuery(BaseGrants):
    def privilege_names(self):
        # TODO: what is insert equivalent?
        return {"select": "roles/bigquery.dataViewer", "insert": "roles/bigquery.dataViewer", "fake_privilege": "roles/invalid"}


    def assert_expected_grants_match_actual(self, project, relation_name, expected_grants):
        actual_grants = self.get_grants_on_relation(project, relation_name)
        # need a case-insensitive comparison
        # so just a simple "assert expected == actual_grants" won't work
        diff_a = BaseContext.diff_of_two_dicts_no_lower(actual_grants, expected_grants)
        diff_b = BaseContext.diff_of_two_dicts_no_lower(expected_grants, actual_grants)
        assert diff_a == diff_b == {}

class TestModelGrantsBigQuery(BaseGrantsBigQuery, BaseModelGrants):
    pass


class TestIncrementalGrantsBigQuery(BaseGrantsBigQuery, BaseIncrementalGrants):
    pass


class TestSeedGrantsBigQuery(BaseGrantsBigQuery, BaseSeedGrants):
    pass


class TestSnapshotGrantsBigQuery(BaseGrantsBigQuery, BaseSnapshotGrants):
    pass


class TestInvalidGrantsBigQuery(BaseGrantsBigQuery, BaseInvalidGrants):
    pass
