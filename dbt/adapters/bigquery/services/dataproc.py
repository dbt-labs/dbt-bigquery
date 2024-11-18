from typing import Dict, Union
import uuid

from google.api_core.retry import Retry
from google.cloud.dataproc_v1 import (
    Batch,
    CreateBatchRequest,
    Job,
    JobControllerClient,
    RuntimeConfig,
    BatchControllerClient,
)
from google.cloud.storage import Client as StorageClient
from google.protobuf.json_format import ParseDict

from dbt.adapters.bigquery.credentials import BigQueryCredentials, DataprocBatchConfig
from dbt.adapters.bigquery.services._config import logger


_DEFAULT_JAR_FILE_URI = "gs://spark-lib/bigquery/spark-bigquery-with-dependencies_2.13-0.34.0.jar"


class DataProcService:
    def __init__(
        self,
        storage_client: StorageClient,
        cluster_client: JobControllerClient = None,
        serverless_client: BatchControllerClient = None,
    ) -> None:
        self._storage_client = storage_client
        self._cluster_client = cluster_client
        self._serverless_client = serverless_client

    def submit_cluster_job(
        self,
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

        logger.info(f"Submitting cluster job to: {cluster_name}")

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
        operation = self._cluster_client.submit_job_as_operation(request)

        # wait for the job to complete
        response: Job = operation.result(polling=retry)

        if response.status.state == 6:
            raise ValueError(response.status.details)

        return response

    def submit_serverless_batch(
        self,
        parsed_model: dict,
        credentials: BigQueryCredentials,
        retry: Retry,
    ) -> Batch:
        batch_id = parsed_model["config"].get("batch_id", str(uuid.uuid4()))

        logger.info(f"Submitting batch job with id: {batch_id}")

        batch = _batch(parsed_model, credentials)

        # Apply configuration from dataproc_batch key, possibly overriding defaults.
        if credentials.dataproc_batch:
            batch = _update_batch_from_config(credentials.dataproc_batch, batch)

        project = credentials.execution_project
        region = credentials.dataproc_region
        request = CreateBatchRequest(
            parent=f"projects/{project}/locations/{region}",
            batch=batch,
            batch_id=batch_id,
        )

        # submit the batch
        operation = self._serverless_client.create_batch(request)

        # wait for the batch to complete
        response: Batch = operation.result(polling=retry)

        return response

    def upload_model(
        self,
        parsed_model: dict,
        compiled_code: str,
        credentials: BigQueryCredentials,
    ) -> None:
        bucket = self._storage_client.get_bucket(credentials.gcs_bucket)

        schema = parsed_model["schema"]
        identifier = parsed_model["alias"]
        blob = bucket.blob(f"{schema}/{identifier}.py")

        blob.upload_from_string(compiled_code)


def _batch(parsed_model: dict, credentials: BigQueryCredentials) -> Batch:
    # create the Dataproc Serverless job config
    # need to pin dataproc version to 1.1 as it now defaults to 2.0
    # https://cloud.google.com/dataproc-serverless/docs/concepts/properties
    # https://cloud.google.com/dataproc-serverless/docs/reference/rest/v1/projects.locations.batches#runtimeconfig
    schema = parsed_model["schema"]
    identifier = parsed_model["alias"]
    path = f"gs://{credentials.gcs_bucket}/{schema}/{identifier}"

    return Batch(
        {
            "runtime_config": RuntimeConfig(
                version="1.1",
                properties={
                    "spark.executor.instances": "2",
                },
            ),
            "pyspark_batch": {
                "main_python_file_uri": path,
                "jar_file_uris": [
                    parsed_model["config"].get("jar_file_uri", _DEFAULT_JAR_FILE_URI)
                ],
            },
        }
    )


def _update_batch_from_config(
    config_dict: Union[Dict, DataprocBatchConfig], target: Batch
) -> Batch:
    try:
        # updates in place
        ParseDict(config_dict, target._pb)
    except Exception as e:
        docurl = (
            "https://cloud.google.com/dataproc-serverless/docs/reference/rpc/google.cloud.dataproc.v1"
            "#google.cloud.dataproc.v1.Batch"
        )
        raise ValueError(
            f"Unable to parse dataproc_batch as valid batch specification. See {docurl}. {str(e)}"
        ) from e
    return target
