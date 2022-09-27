import pytest
from dbt.tests.adapter.utils import test_timestamps


class TestCurrentTimestampSnowflake(test_timestamps.TestCurrentTimestamps):
    @pytest.fixture(scope="class")
    def expected_schema(self):
        return {
            "current_timestamp": "TIMESTAMP",
            "current_timestamp_in_utc": "TIMESTAMP",
            "current_timestamp_backcompat": "TIMESTAMP",
        }
