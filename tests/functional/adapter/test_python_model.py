import os
import pytest
from dbt.tests.util import run_dbt, write_file
import dbt.tests.adapter.python_model.test_python_model as dbt_tests

TEST_SKIP_MESSAGE = "Skipping the Tests since Dataproc serverless is not stable. " \
                    "TODO: Fix later"


@pytest.mark.skip(reason=TEST_SKIP_MESSAGE)
class TestPythonModelDataproc(dbt_tests.BasePythonModelTests):
    pass


@pytest.mark.skip(reason=TEST_SKIP_MESSAGE)
class TestPythonIncrementalMatsDataproc(dbt_tests.BasePythonIncrementalTests):
    pass


models__simple_python_model = """
import pandas

def model(dbt, spark):
    dbt.config(
        materialized='table',
    )
    data = [[1,2]] * 10
    return spark.createDataFrame(data, schema=['test', 'test2'])
"""

models__simple_python_model_v2 = """
import pandas

def model(dbt, spark):
    dbt.config(
        materialized='table',
    )
    data = [[1,2]] * 10
    return spark.createDataFrame(data, schema=['test1', 'test3'])
"""


@pytest.mark.skip(reason=TEST_SKIP_MESSAGE)
class TestChangingSchemaDataproc:

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "simple_python_model.py": models__simple_python_model
        }

    def test_changing_schema(self, project, logs_dir):
        run_dbt(["run"])
        write_file(models__simple_python_model_v2, project.project_root + '/models', "simple_python_model.py")
        run_dbt(["run"])
        log_file = os.path.join(logs_dir, "dbt.log")
        with open(log_file, "r") as f:
            log = f.read()
            # validate #5510 log_code_execution works
            assert "On model.test.simple_python_model:" in log
            assert "return spark.createDataFrame(data, schema=['test1', 'test3'])" in log
            assert "Execution status: OK in" in log
