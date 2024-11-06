from typing import Dict, Union
import uuid

from google.cloud.dataproc_v1 import (
    Batch,
    CreateBatchRequest,
    GetBatchRequest,
    Job,
    RuntimeConfig,
)

from dbt.adapters.base import PythonJobHelper
from dbt.adapters.events.logging import AdapterLogger
from google.protobuf.json_format import ParseDict

from dbt.adapters.bigquery.credentials import BigQueryCredentials, DataprocBatchConfig
from dbt.adapters.bigquery.clients import (
    batch_controller_client,
    job_controller_client,
    storage_client,
)
from dbt.adapters.bigquery.retry import RetryFactory


_logger = AdapterLogger("BigQuery")


_DEFAULT_JAR_FILE_URI = "gs://spark-lib/bigquery/spark-bigquery-with-dependencies_2.13-0.34.0.jar"


class BaseDataProcHelper(PythonJobHelper):
    def __init__(self, parsed_model: Dict, credentials: BigQueryCredentials) -> None:
        """_summary_

        Args:
            credentials (_type_): _description_
        """
        # validate all additional stuff for python is set
        for required_config in ["dataproc_region", "gcs_bucket"]:
            if not getattr(credentials, required_config):
                raise ValueError(
                    f"Need to supply {required_config} in profile to submit python job"
                )

        self._storage_client = storage_client(credentials)
        self._project = credentials.execution_project
        self._region = credentials.dataproc_region

        schema = parsed_model["schema"]
        identifier = parsed_model["alias"]
        self._model_file_name = f"{schema}/{identifier}.py"
        self._gcs_bucket = credentials.gcs_bucket
        self._gcs_path = f"gs://{credentials.gcs_bucket}/{self._model_file_name}"

        # set retry policy, default to timeout after 24 hours
        retry = RetryFactory(credentials)
        self._polling_retry = retry.polling(timeout=parsed_model["config"].get("timeout"))

    def _upload_to_gcs(self, compiled_code: str) -> None:
        bucket = self._storage_client.get_bucket(self._gcs_bucket)
        blob = bucket.blob(self._model_file_name)
        blob.upload_from_string(compiled_code)

    def submit(self, compiled_code: str) -> Job:
        self._upload_to_gcs(compiled_code)
        return self._submit_dataproc_job()

    def _submit_dataproc_job(self) -> Job:
        raise NotImplementedError("_submit_dataproc_job not implemented")


class ClusterDataprocHelper(BaseDataProcHelper):
    def __init__(self, parsed_model: Dict, credentials: BigQueryCredentials) -> None:
        super().__init__(parsed_model, credentials)
        self._job_controller_client = job_controller_client(credentials)
        self._cluster_name = parsed_model["config"].get(
            "dataproc_cluster_name", credentials.dataproc_cluster_name
        )

        if not self._cluster_name:
            raise ValueError(
                "Need to supply dataproc_cluster_name in profile or config to submit python job with cluster submission method"
            )

    def _submit_dataproc_job(self) -> Job:
        job = {
            "placement": {"cluster_name": self._cluster_name},
            "pyspark_job": {
                "main_python_file_uri": self._gcs_path,
            },
        }
        operation = self._job_controller_client.submit_job_as_operation(
            request={
                "project_id": self._project,
                "region": self._region,
                "job": job,
            }
        )
        # check if job failed
        response = operation.result(polling=self._polling_retry)
        if response.status.state == 6:
            raise ValueError(response.status.details)
        return response


class ServerlessDataProcHelper(BaseDataProcHelper):
    def __init__(self, parsed_model: Dict, credentials: BigQueryCredentials) -> None:
        super().__init__(parsed_model, credentials)
        self._batch_controller_client = batch_controller_client(credentials)
        self._batch_id = parsed_model["config"].get("batch_id", str(uuid.uuid4()))
        self._jar_file_uri = parsed_model["config"].get("jar_file_uri", _DEFAULT_JAR_FILE_URI)
        self._dataproc_batch = credentials.dataproc_batch

    def _submit_dataproc_job(self) -> Batch:
        _logger.info(f"Submitting batch job with id: {self._batch_id}")

        # make the request
        request = CreateBatchRequest(
            parent=f"projects/{self._project}/locations/{self._region}",
            batch=self._configure_batch(),
            batch_id=self._batch_id,
        )
        self._batch_controller_client.create_batch(request=request)

        # return the response
        batch = GetBatchRequest(f"{request.parent}/batches/{self._batch_id}")
        return self._batch_controller_client.get_batch(batch, retry=self._polling_retry)
        # there might be useful results here that we can parse and return
        # Dataproc job output is saved to the Cloud Storage bucket
        # allocated to the job. Use regex to obtain the bucket and blob info.
        # matches = re.match("gs://(.*?)/(.*)", response.driver_output_resource_uri)
        # output = (
        #     self.storage_client
        #     .get_bucket(matches.group(1))
        #     .blob(f"{matches.group(2)}.000000000")
        #     .download_as_string()
        # )

    def _configure_batch(self) -> Batch:
        # create the Dataproc Serverless job config
        # need to pin dataproc version to 1.1 as it now defaults to 2.0
        # https://cloud.google.com/dataproc-serverless/docs/concepts/properties
        # https://cloud.google.com/dataproc-serverless/docs/reference/rest/v1/projects.locations.batches#runtimeconfig
        batch = Batch(
            {
                "runtime_config": RuntimeConfig(
                    version="1.1",
                    properties={
                        "spark.executor.instances": "2",
                    },
                ),
                "pyspark_batch": {
                    "main_python_file_uri": self._gcs_path,
                    "jar_file_uris": [self._jar_file_uri],
                },
            }
        )

        # Apply configuration from dataproc_batch key, possibly overriding defaults.
        if self._dataproc_batch:
            batch = _update_batch_from_config(self._dataproc_batch, batch)

        return batch


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
