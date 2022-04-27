import pytest

from dbt.tests.adapter.basic.test_base import BaseSimpleMaterializations
from dbt.tests.adapter.basic.test_singular_tests import BaseSingularTests
from dbt.tests.adapter.basic.test_singular_tests_ephemeral import (
    BaseSingularTestsEphemeral,
)
from dbt.tests.adapter.basic.test_empty import BaseEmpty
from dbt.tests.adapter.basic.test_ephemeral import BaseEphemeral
from dbt.tests.adapter.basic.test_incremental import BaseIncremental
from dbt.tests.adapter.basic.test_generic_tests import BaseGenericTests
from dbt.tests.adapter.basic.test_snapshot_check_cols import BaseSnapshotCheckCols
from dbt.tests.adapter.basic.test_snapshot_timestamp import BaseSnapshotTimestamp
from dbt.tests.adapter.basic.test_adapter_methods import BaseAdapterMethod


class TestSimpleMaterializationsBigQuery(BaseSimpleMaterializations):
    # This test requires a full-refresh to replace a table with a view
    @pytest.fixture(scope="class")
    def test_config(self):
        return {"require_full_refresh": True}


class TestSingularTestsBigQuery(BaseSingularTests):
    pass


class TestSingularTestsEphemeralBigQuery(BaseSingularTestsEphemeral):
    pass


class TestEmptyBigQuery(BaseEmpty):
    pass


class TestEphemeralBigQuery(BaseEphemeral):
    pass


class TestIncrementalBigQuery(BaseIncremental):
    pass


class TestGenericTestsBigQuery(BaseGenericTests):
    pass


class TestSnapshotCheckColsBigQuery(BaseSnapshotCheckCols):
    pass


class TestSnapshotTimestampBigQuery(BaseSnapshotTimestamp):
    pass

class TestBaseAdapterMethodBigQuery(BaseAdapterMethod):
    pass


# one time thing for validate connection in 1.1.latest
# proper test at https://github.com/dbt-labs/dbt-core/blob/main/tests/adapter/dbt/tests/adapter/basic/test_validate_connection.py
import dbt.task.debug
class TestValidateConnection:
    # project need to be here otherwise some other tests might break
    def test_validate_connection(self, project, dbt_profile_data):

        dbt.task.debug.DebugTask.validate_connection(
            dbt_profile_data["test"]["outputs"]["default"]
        )