from dbt.tests.adapter.grants.base_grants import BaseGrants
from dbt.tests.adapter.grants.test_model_grants import BaseModelGrants
from dbt.tests.adapter.grants.test_incremental_grants import BaseIncrementalGrants
from dbt.tests.adapter.grants.test_invalid_grants import BaseInvalidGrants
from dbt.tests.adapter.grants.test_seed_grants import BaseSeedGrants
from dbt.tests.adapter.grants.test_snapshot_grants import BaseSnapshotGrants


class BaseGrantsBigQuery(BaseGrants):
    def privilege_grantee_name_overrides(self):
        return {
            "select": "roles/bigquery.dataViewer",
            "insert": "roles/bigquery.dataEditor",
            "fake_privilege": "roles/invalid",
            "invalid_user": "user:fake@dbtlabs.com",
        }


class TestModelGrantsBigQuery(BaseGrantsBigQuery, BaseModelGrants):
    pass


class TestIncrementalGrantsBigQuery(BaseGrantsBigQuery, BaseIncrementalGrants):
    pass


class TestSeedGrantsBigQuery(BaseGrantsBigQuery, BaseSeedGrants):
    # seeds in dbt-bigquery are always "full refreshed," in such a way that
    # the grants do not carry over
    def seeds_support_partial_refresh(self):
        return False


class TestSnapshotGrantsBigQuery(BaseGrantsBigQuery, BaseSnapshotGrants):
    pass


class TestInvalidGrantsBigQuery(BaseGrantsBigQuery, BaseInvalidGrants):
    def grantee_does_not_exist_error(self):
        return "User fake@dbtlabs.com does not exist."

    def privilege_does_not_exist_error(self):
        return "Role roles/invalid is not supported for this resource."
