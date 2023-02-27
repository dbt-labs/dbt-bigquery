import os
from pathlib import Path

import pytest

from dbt.tests.util import run_dbt
from tests.functional.adapter.incremental.fixtures.expected import INCREMENTAL_SEEDS

_TEST_CASES = [
    ('incremental_merge_range', 'merge_expected'),
    ('incremental_merge_time', 'merge_expected'),
    ('incremental_overwrite_day_time', 'incremental_overwrite_time_expected'),
    ('incremental_overwrite_date', 'incremental_overwrite_date_expected'),
    ('incremental_overwrite_partitions', 'incremental_overwrite_date_expected'),
    ('incremental_overwrite_day', 'incremental_overwrite_day_expected'),
    ('incremental_overwrite_range', 'incremental_overwrite_range_expected'),
    ('incremental_overwrite_day_with_copy_partitions', 'incremental_overwrite_day_expected'),
    ('incremental_overwrite_time', 'incremental_overwrite_day_with_time_partition_expected')
]

FILE_DIR = os.path.dirname(os.path.realpath(__file__))


def _open_file(filename: str):
    with open(Path(f"{FILE_DIR}/fixtures/{filename}.sql")) as f:
        result = f.read()
    return result


class TestBigQueryIncrementalStrategies:
    @pytest.fixture(scope="class")
    def models(self):
        return {f"{case_name[0]}.sql": _open_file(case_name[0]) for case_name in _TEST_CASES}

    @pytest.fixture(scope="class")
    def seeds(self):
        return INCREMENTAL_SEEDS

    def test_valid_incremental_strategies_succeed(self, project):
        results = run_dbt()
        assert len(results) == 9

        results = run_dbt()
        assert len(results) == 9
