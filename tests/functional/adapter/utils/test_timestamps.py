import pytest
from dbt.tests.adapter.utils import test_timestamps


class TestCurrentTimestampSnowflake(test_timestamps.TestCurrentTimestamps):
    @pytest.fixture(scope="class")
    def expected_schema(self):
        return {
                "CURRENT_TIMESTAMP": "TIMESTAMP",
                "CURRENT_TIMESTAMP_IN_UTC": "TIMESTAMP"
            }
