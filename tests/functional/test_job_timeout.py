import pytest
from dbt.tests.util import run_dbt

_MODEL_SQL = """
select 1 as id
"""

_DEFAULT_TIMEOUT = 300
_SHORT_TIMEOUT = 1


class BaseJobTimeout:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "model.sql": _MODEL_SQL,
        }


class TestSuccessfulJobRun(BaseJobTimeout):
    def test_bigquery_default_job_run(self, project):
        run_dbt()


class TestJobTimeout(BaseJobTimeout):
    @pytest.fixture(scope="class")
    def profile_config_update(self, dbt_profile_target):
        outputs = {"default": dbt_profile_target}
        outputs["default"]["job_execution_timeout_seconds"] = _SHORT_TIMEOUT
        return {"test": {"outputs": outputs, "target": "default"}}

    def get_ctx_vars(self, project):
        fields = [
            "method",
            "database",
            "execution_project",
            "schema",
            "location",
            "priority",
            "maximum_bytes_billed",
            "impersonate_service_account",
            "job_retry_deadline_seconds",
            "job_retries",
            "job_creation_timeout_seconds",
            "job_execution_timeout_seconds",
            "keyfile",
            "keyfile_json",
            "timeout_seconds",
            "token",
            "refresh_token",
            "client_id",
            "client_secret",
            "token_uri",
            "dataproc_region",
            "dataproc_cluster_name",
            "gcs_bucket",
            "dataproc_batch",
        ]
        field_list = ", ".join(['"{}"'.format(f) for f in fields])
        query = "select {field_list} from {schema}.context".format(
            field_list=field_list, schema=project.test_schema
        )
        vals = project.run_sql(query, fetch="all")
        ctx = dict([(k, v) for (k, v) in zip(fields, vals[0])])
        return ctx

    def test_job_timeout(self, project):
        run_dbt()
        ctx = self.get_ctx_vars(project)
        assert ctx["target.job_execution_timeout_seconds"] == 1
