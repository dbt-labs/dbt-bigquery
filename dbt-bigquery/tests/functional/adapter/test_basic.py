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
from dbt.tests.adapter.basic.test_validate_connection import BaseValidateConnection
from dbt.tests.adapter.basic.test_docs_generate import BaseDocsGenerate
from dbt.tests.adapter.basic.expected_catalog import base_expected_catalog
from tests.functional.adapter.expected_stats import bigquery_stats


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


class TestBigQueryValidateConnection(BaseValidateConnection):
    pass


class TestDocsGenerateBigQuery(BaseDocsGenerate):
    @pytest.fixture(scope="class")
    def expected_catalog(self, project):
        return base_expected_catalog(
            project,
            role=None,
            id_type="INT64",
            text_type="STRING",
            time_type="DATETIME",
            view_type="view",
            table_type="table",
            model_stats=bigquery_stats(False),
            seed_stats=bigquery_stats(True),
        )
