import pytest

from dbt.tests.adapter.grants.base_grants import BaseGrants
from dbt.tests.adapter.grants.test_model_grants import BaseModelGrants
from dbt.tests.adapter.grants.test_incremental_grants import BaseIncrementalGrants
from dbt.tests.adapter.grants.test_invalid_grants import BaseInvalidGrants
from dbt.tests.adapter.grants.test_seed_grants import BaseSeedGrants
from dbt.tests.adapter.grants.test_snapshot_grants import BaseSnapshotGrants


class BaseGrantsBigQuery(BaseGrants):
    def privilege_names(self):
        # TODO: what is insert equivalent?
        return {"select": "roles/dataViewer", "insert": "roles/dataViewer", "fake_privilege": "roles/invalid"}


class TestModelGrantsBigQuery(BaseModelGrants):
    pass


class TestIncrementalGrantsBigQuery(BaseIncrementalGrants):
    pass


class TestSeedGrantsBigQuery(BaseSeedGrants):
    pass


class TestSnapshotGrantsBigQuery(BaseSnapshotGrants):
    pass


class TestInvalidGrantsBigQuery(BaseInvalidGrants):
    pass