import json
import os
import pytest

from pathlib import Path
from pytest import mark

from dbt.tests.util import run_dbt, rm_file, write_file, check_relations_equal

from dbt.tests.adapter.simple_copy.test_simple_copy import (
   SimpleCopySetup,
   SimpleCopyBase
)

from tests.functional.adapter.simple_copy.fixtures import (
    _MODELS_INCREMENTAL_UPDATE_COLS,
    _SEEDS__SEED_MERGE_COLS_INITIAL,
    _SEEDS__SEED_MERGE_COLS_UPDATE,
    _SEEDS__SEED_MERGE_COLS_EXPECTED_RESULT,
)

class TestSimpleCopyBase(SimpleCopyBase):
    pass


class TestIncrementalMergeColumns:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "incremental_update_cols.sql": _MODELS_INCREMENTAL_UPDATE_COLS
        }

    @pytest.fixture(scope="class")
    def seeds(self):
        return {"seed.csv": _SEEDS__SEED_MERGE_COLS_INITIAL}

    def test_incremental_merge_columns(self, project):
        run_dbt(["seed"])
        run_dbt(["run"])

        main_seed_file = project.project_root / Path("seeds") / Path("seed.csv")
        expected_seed_file = project.project_root / Path("seeds") / Path("expected_result.csv")
        rm_file(main_seed_file)
        write_file(_SEEDS__SEED_MERGE_COLS_UPDATE, main_seed_file)
        write_file(_SEEDS__SEED_MERGE_COLS_EXPECTED_RESULT, expected_seed_file)

        run_dbt(["seed"])
        run_dbt(["run"])
        check_relations_equal(
            project.adapter, ["incremental_update_cols", "expected_result"]
        )
