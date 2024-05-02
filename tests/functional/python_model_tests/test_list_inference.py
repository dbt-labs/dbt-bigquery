"""
This test case addresses this regression: https://github.com/dbt-labs/dbt-bigquery/issues/1047

As the comments point out, the issue appears to be that the default settings are:
    - list inference: off
    - intermediate format: parquet

Adjusting either of these alleviates the issue.

When the regression was first reported, `models.MULTI_RECORD` failed while the other three models passed.
"""
from dbt.tests.util import run_dbt_and_capture
import pytest

from tests.functional.python_model_tests import models


class ListInference:
    expect_pass = True

    def test_model(self, project):
        result, output = run_dbt_and_capture(["run"], expect_pass=self.expect_pass)
        assert len(result) == 1


class TestPythonSingleRecord(ListInference):
    @pytest.fixture(scope="class")
    def models(self):
        return {"model.py": models.SINGLE_RECORD}


class TestPythonMultiRecordDefault(ListInference):
    @pytest.fixture(scope="class")
    def models(self):
        # this is the model that initially failed for this issue
        return {"model.py": models.MULTI_RECORD_DEFAULT}


class TestPythonDisableListInference(ListInference):
    expect_pass = False

    @pytest.fixture(scope="class")
    def models(self):
        # this model mimics what was happening before defaulting enable_list_inference=True
        return {"model.py": models.DISABLE_LIST_INFERENCE}


class TestPythonEnableListInference(ListInference):
    @pytest.fixture(scope="class")
    def models(self):
        return {"model.py": models.ENABLE_LIST_INFERENCE}


class TestPythonOrcFormat(ListInference):
    @pytest.fixture(scope="class")
    def models(self):
        return {"model.py": models.ORC_FORMAT}


class TestPythonDisableListInferenceOrcFormat(ListInference):
    @pytest.fixture(scope="class")
    def models(self):
        return {"model.py": models.DISABLE_LIST_INFERENCE_ORC_FORMAT}


class TestPythonEnableListInferenceParquetFormat(ListInference):
    @pytest.fixture(scope="class")
    def models(self):
        # this is the model that initially failed for this issue
        return {"model.py": models.ENABLE_LIST_INFERENCE_PARQUET_FORMAT}
