from typing import Dict, Union
import uuid

from google.api_core.retry import Retry
from google.cloud.dataproc_v1 import (
    Batch,
    CreateBatchRequest,
    RuntimeConfig,
    BatchControllerClient,
)
from google.protobuf.json_format import ParseDict

from dbt.adapters.events.logging import AdapterLogger

from dbt.adapters.bigquery.credentials import BigQueryCredentials, DataprocBatchConfig


_logger = AdapterLogger("BigQuery")


_DEFAULT_JAR_FILE_URI = "gs://spark-lib/bigquery/spark-bigquery-with-dependencies_2.13-0.34.0.jar"


def submit_serverless_batch(
    client: BatchControllerClient,
    parsed_model: dict,
    credentials: BigQueryCredentials,
    retry: Retry,
) -> Batch:
    batch_id = parsed_model["config"].get("batch_id", str(uuid.uuid4()))

    _logger.info(f"Submitting batch job with id: {batch_id}")

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
    operation = client.create_batch(request)

    # wait for the batch to complete
    response: Batch = operation.result(polling=retry)

    return response


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
