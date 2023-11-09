import os
import pytest
import time
from dbt.tests.util import run_dbt, run_dbt_and_capture, write_file
import dbt.tests.adapter.python_model.test_python_model as dbt_tests

TEST_SKIP_MESSAGE = (
    "Skipping the Tests since Dataproc serverless is not stable. " "TODO: Fix later"
)

blocks_for_thirty_sec = """
def model(dbt, _):
    dbt.config(
        materialized='table',
        timeout=5
    )
    import pandas as pd
    data = {'col_1': [3, 2, 1, 0], 'col_2': ['a', 'b', 'c', 'd']}
    df = pd.DataFrame.from_dict(data)
    import time
    time.sleep(30)
    return df
"""


class TestPythonModelDataprocTimeoutTest:
    @pytest.fixture(scope="class")
    def models(self):
        return {"30_sec_python_model.py": blocks_for_thirty_sec}

    def test_model_times_out(self, project):
        result, output = run_dbt_and_capture(["run"], expect_pass=False)
        assert len(result) == 1
        assert "Operation did not complete within the designated timeout of 5 seconds." in output


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

macro__partition_count_sql = """
{% test number_partitions(model, expected) %}

    {%- set result = get_partitions_metadata(model) %}

    {% if result %}
        {% set partitions = result.columns['partition_id'].values() %}
    {% else %}
        {% set partitions = () %}
    {% endif %}

    {% set actual = partitions | length %}
    {% set success = 1 if model and actual == expected else 0 %}

    select 'Expected {{ expected }}, but got {{ actual }}' as validation_error
    from (select true)
    where {{ success }} = 0

{% endtest %}
"""

models__partitioned_model_python = """
import pandas as pd

def model(dbt, spark):
    dbt.config(
        materialized='table',
        partition_by={
                "field": "C",
                "data_type": "timestamp",
                "granularity": "day",
            },
        cluster_by=["A"],
    )
    random_array = [
        ["A", -157.9871329592354],
        ["B", -528.9769041860632],
        ["B", 941.0504221837489],
        ["B", 919.5903586746183],
        ["A", -121.25678519054622],
        ["A", 254.9985130814921],
        ["A", 833.2963094260072],
    ]

    df = pd.DataFrame(random_array, columns=["A", "B"])

    df["C"] = pd.to_datetime('now')

    final_df = df[["A", "B", "C"]]

    return final_df
"""

models__partitioned_model_yaml = """
models:
  - name: python_partitioned_model
    description: A random table with a calculated column defined in python.
    config:
      batch_id: '{{ run_started_at.strftime("%Y-%m-%d-%H-%M-%S") }}-python-partitioned'
    tests:
      - number_partitions:
          expected: "{{ var('expected', 1) }}"
    columns:
      - name: A
        description: Column A
      - name: B
        description: Column B
      - name: C
        description: Column C
"""


class TestPythonPartitionedModels:
    @pytest.fixture(scope="class")
    def macros(self):
        return {"partition_metadata.sql": macro__partition_count_sql}

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "python_partitioned_model.py": models__partitioned_model_python,
            "python_partitioned_model.yml": models__partitioned_model_yaml,
        }

    def test_multiple_named_python_models(self, project):
        result = run_dbt(["run"])
        assert len(result) == 1

        test_results = run_dbt(["test"])
        for result in test_results:
            assert result.status == "pass"
            assert not result.skipped
            assert result.failures == 0


models__simple_python_model_v2 = """
import pandas

def model(dbt, spark):
    dbt.config(
        materialized='table',
    )
    data = [[1,2]] * 10
    return spark.createDataFrame(data, schema=['test1', 'test3'])
"""

models__python_array_batch_id_python = """
import pandas as pd

def model(dbt, spark):
    random_array = [
        [9001.3985362160208, -157.9871329592354],
        [-817.8786101352823, -528.9769041860632],
        [-886.6488625065194, 941.0504221837489],
        [6.69525238666165, 919.5903586746183],
        [754.3718741592056, -121.25678519054622],
        [-352.3158889341157, 254.9985130814921],
        [563.0633042715097, 833.2963094260072],
    ]

    df = pd.DataFrame(random_array, columns=["A", "B"])

    df["C"] = df["A"] * df["B"]

    final_df = df[["A", "B", "C"]]

    return final_df
"""

models__python_array_batch_id_yaml = """
models:
  - name: python_array_batch_id
    description: A random table with a calculated column defined in python.
    config:
      batch_id: '{{ run_started_at.strftime("%Y-%m-%d-%H-%M-%S") }}-python-array'
    columns:
      - name: A
        description: Column A
      - name: B
        description: Column B
      - name: C
        description: Column C
"""

custom_ts_id = str("custom-" + str(time.time()).replace(".", "-"))

models__bad_python_array_batch_id_yaml = f"""
models:
  - name: python_array_batch_id
    description: A random table with a calculated column defined in python.
    config:
      batch_id: {custom_ts_id}-python-array
    columns:
      - name: A
        description: Column A
      - name: B
        description: Column B
      - name: C
        description: Column C
"""


@pytest.mark.skip(reason="Currently failing as run_started_at is the same across dbt runs")
class TestPythonBatchIdModels:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "python_array_batch_id.py": models__python_array_batch_id_python,
            "python_array_batch_id.yml": models__python_array_batch_id_yaml,
        }

    def test_multiple_named_python_models(self, project):
        result, output = run_dbt_and_capture(["run"], expect_pass=True)
        time.sleep(5)  # In case both runs are submitted simultaneously
        result_two, output_two = run_dbt_and_capture(["run"], expect_pass=True)
        assert len(result) == 1
        assert len(result_two) == 1


class TestPythonDuplicateBatchIdModels:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "python_array_batch_id.py": models__python_array_batch_id_python,
            "python_array_batch_id.yml": models__bad_python_array_batch_id_yaml,
        }

    def test_multiple_python_models_fixed_id(self, project):
        result, output = run_dbt_and_capture(["run"], expect_pass=True)
        result_two, output_two = run_dbt_and_capture(["run"], expect_pass=False)
        assert result_two[0].message.startswith("409 Already exists: Failed to create batch:")
        assert len(result) == 1
        assert len(result_two) == 1


@pytest.mark.skip(reason=TEST_SKIP_MESSAGE)
class TestChangingSchemaDataproc:
    @pytest.fixture(scope="class")
    def models(self):
        return {"simple_python_model.py": models__simple_python_model}

    def test_changing_schema(self, project, logs_dir):
        run_dbt(["run"])
        write_file(
            models__simple_python_model_v2,
            project.project_root + "/models",
            "simple_python_model.py",
        )
        run_dbt(["run"])
        log_file = os.path.join(logs_dir, "dbt.log")
        with open(log_file, "r") as f:
            log = f.read()
            # validate #5510 log_code_execution works
            assert "On model.test.simple_python_model:" in log
            assert "return spark.createDataFrame(data, schema=['test1', 'test3'])" in log
            assert "Execution status: OK in" in log
