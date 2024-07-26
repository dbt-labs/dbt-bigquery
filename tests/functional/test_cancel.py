import os
import signal
import time
import subprocess

import pytest
from dbt.tests.util import get_connection

_LONG_RUNNING_MODEL_SQL = """
    {{ config(materialized='table') }}
    with array_1 as (
    select generated_ids from UNNEST(GENERATE_ARRAY(1, 200000)) AS generated_ids
    ),
    array_2 as (
    select generated_ids from UNNEST(GENERATE_ARRAY(2, 200000)) AS generated_ids
    )

    SELECT array_1.generated_ids
    FROM array_1
    LEFT JOIN array_1 as jnd on 1=1
    LEFT JOIN array_2 as jnd2 on 1=1
    LEFT JOIN array_1 as jnd3 on jnd3.generated_ids >= jnd2.generated_ids
"""


def _get_info_schema_jobs_query(project_id, dataset_id, table_id):
    """
    Running this query requires roles/bigquery.resourceViewer on the project,
    see: https://cloud.google.com/bigquery/docs/information-schema-jobs#required_role
    :param project_id:
    :param dataset_id:
    :param table_id:
    :return: a single job id that matches the model we tried to create and was cancelled
    """
    return f"""
        SELECT job_id
        FROM `region-us`.`INFORMATION_SCHEMA.JOBS_BY_PROJECT`
        WHERE creation_time > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 5 HOUR)
        AND statement_type = 'CREATE_TABLE_AS_SELECT'
        AND state = 'DONE'
        AND job_id IS NOT NULL
        AND project_id = '{project_id}'
        AND error_result.reason = 'stopped'
        AND error_result.message = 'Job execution was cancelled: User requested cancellation'
        AND destination_table.table_id = '{table_id}'
        AND destination_table.dataset_id = '{dataset_id}'
    """


class TestBigqueryCancelsQueriesOnKeyboardInterrupt:
    @pytest.fixture(scope="class", autouse=True)
    def models(self):
        return {
            "model.sql": _LONG_RUNNING_MODEL_SQL,
        }

    def test_bigquery_cancels_queries_on_keyboard_interrupt(self, project):
        os.chdir(project.project_root)
        run_dbt_process = subprocess.Popen(
            [
                "dbt",
                "run",
                "--profiles-dir",
                project.profiles_dir,
                "--project-dir",
                project.project_root,
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            shell=False,
        )
        std_out_log = ""
        while True:
            std_out_line = run_dbt_process.stdout.readline().decode("utf-8")
            std_out_log += std_out_line
            if std_out_line != "":
                print(std_out_line)
                if "START sql" in std_out_line:
                    time.sleep(1)
                    run_dbt_process.send_signal(signal.SIGINT)

            if run_dbt_process.poll():
                break

        assert "CANCEL query model.test.model" in std_out_log
        # Because we run this in a subprocess we have to actually call Bigquery and look up the job id
        with get_connection(project.adapter):
            job_id = project.run_sql(
                _get_info_schema_jobs_query(project.database, project.test_schema, "model")
            )
        assert len(job_id) == 1
