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

from tests.functional.python_model_tests import files


class TestListInference:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            # this is what worked prior to this issue
            "single_record.py": files.SINGLE_RECORD,
            # this is the model that initially failed for this issue
            "multi_record.py": files.MULTI_RECORD_DEFAULT,
            # these are explicit versions of the default settings
            "enable_list_inference.py": files.ENABLE_LIST_INFERENCE,
            "enable_list_inference_parquet_format.py": files.ENABLE_LIST_INFERENCE_PARQUET_FORMAT,
            # orc format also resolves the issue, regardless of list inference
            "orc_format.py": files.ORC_FORMAT,
            "disable_list_inference_orc_format.py": files.DISABLE_LIST_INFERENCE_ORC_FORMAT,
        }

    def test_models_success(self, project, models):
        result, output = run_dbt_and_capture(["run"])
        assert len(result) == len(models)
