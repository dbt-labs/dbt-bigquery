from typing import Dict

from google.api_core.retry import Retry
from google.cloud.dataproc_v1 import Job, JobControllerClient

from dbt.adapters.events.logging import AdapterLogger

from dbt.adapters.bigquery.credentials import BigQueryCredentials


_logger = AdapterLogger("BigQuery")


def submit_cluster_job(
    client: JobControllerClient,
    parsed_model: Dict,
    credentials: BigQueryCredentials,
    retry: Retry,
) -> Job:
    cluster_name = parsed_model["config"].get(
        "dataproc_cluster_name", credentials.dataproc_cluster_name
    )
    if not cluster_name:
        raise ValueError(
            "Need to supply dataproc_cluster_name in profile or config to submit python job with cluster submission method"
        )

    _logger.info(f"Submitting cluster job to: {cluster_name}")

    schema = parsed_model["schema"]
    identifier = parsed_model["alias"]
    request = {
        "project_id": credentials.execution_project,
        "region": credentials.dataproc_region,
        "job": {
            "placement": {"cluster_name": cluster_name},
            "pyspark_job": {
                "main_python_file_uri": f"gs://{credentials.gcs_bucket}/{schema}/{identifier}.py",
            },
        },
    }

    # submit the job
    operation = client.submit_job_as_operation(request)

    # wait for the job to complete
    response: Job = operation.result(polling=retry)

    if response.status.state == 6:
        raise ValueError(response.status.details)

    return response
