import pytest

from google.cloud.bigquery.client import Client

from dbt.tests.util import run_dbt


_MACRO__BQ_LABELS = """
{% macro bq_labels() %}{
    "system": "{{ env_var('LABEL_SYSTEM', 'my_system') }}",
    "env_type": "{{ env_var('LABEL_ENV', 'dev') }}"
}{% endmacro %}
"""
_MODEL__MY_TABLE = """
{{ config(materialized= "table") }}
select 1 as id
"""


class TestQueryCommentJobLabel:
    @pytest.fixture(scope="class")
    def models(self):
        return {"my_table.sql": _MODEL__MY_TABLE}

    @pytest.fixture(scope="class")
    def macros(self):
        return {"bq_labels.sql": _MACRO__BQ_LABELS}

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "query-comment": {
                "comment": "{{ bq_labels() }}",
                "job-label": True,
                "append": True,
            }
        }

    def test_query_comments_displays_as_job_labels(self, project):
        """
        Addresses this regression in dbt-bigquery 1.6:
        https://github.com/dbt-labs/dbt-bigquery/issues/863
        """
        results = run_dbt(["run"])
        job_id = results.results[0].adapter_response.get("job_id")
        with project.adapter.connection_named("_test"):
            client: Client = project.adapter.connections.get_thread_connection().handle
            job = client.get_job(job_id=job_id)

        # this is what should happen
        assert job.labels.get("system") == "my_system"
        assert job.labels.get("env_type") == "dev"
