import pytest
from dbt.tests.adapter.utils.test_timestamps import BaseCurrentTimestamps


class TestCurrentTimestampBigQuery(BaseCurrentTimestamps):
    @pytest.fixture(scope="class")
    def expected_schema(self):
        return {
            "current_timestamp": "TIMESTAMP",
            "current_timestamp_in_utc_backcompat": "TIMESTAMP",
            "current_timestamp_backcompat": "TIMESTAMP",
        }

    @pytest.fixture(scope="class")
    def expected_sql(self):
        return """select current_timestamp() as current_timestamp,
                current_timestamp as current_timestamp_in_utc_backcompat,
                current_timestamp as current_timestamp_backcompat"""