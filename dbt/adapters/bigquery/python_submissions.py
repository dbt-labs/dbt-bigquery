from typing import Dict, Union

from dbt.adapters.base import PythonJobHelper
from google.api_core.future.polling import POLLING_PREDICATE

from dbt.adapters.bigquery import BigQueryConnectionManager, BigQueryCredentials
from dbt.adapters.bigquery.connections import DataprocBatchConfig
from google.api_core import retry
from google.api_core.client_options import ClientOptions
from google.cloud import storage, dataproc_v1  # type: ignore
from google.protobuf.json_format import ParseDict

OPERATION_RETRY_TIME = 10


class BaseDataProcHelper(PythonJobHelper):
    def __init__(self, parsed_model: Dict, credential: BigQueryCredentials) -> None:
        """_summary_

        Args:
            credential (_type_): _description_
        """
        # validate all additional stuff for python is set
        schema = parsed_model["schema"]
        identifier = parsed_model["alias"]
        self.parsed_model = parsed_model
        python_required_configs = [
            "dataproc_region",
            "gcs_bucket",
        ]
        for required_config in python_required_configs:
            if not getattr(credential, required_config):
                raise ValueError(
                    f"Need to supply {required_config} in profile to submit python job"
                )
        self.model_file_name = f"{schema}/{identifier}.py"
        self.credential = credential
        self.GoogleCredentials = BigQueryConnectionManager.get_credentials(credential)
        self.storage_client = storage.Client(
            project=self.credential.execution_project, credentials=self.GoogleCredentials
        )
        self.gcs_location = "gs://{}/{}".format(self.credential.gcs_bucket, self.model_file_name)

        # set retry policy, default to timeout after 24 hours
        self.timeout = self.parsed_model["config"].get(
            "timeout", self.credential.job_execution_timeout_seconds or 60 * 60 * 24
        )
        self.result_polling_policy = retry.Retry(
            predicate=POLLING_PREDICATE, maximum=10.0, timeout=self.timeout
        )
        self.client_options = ClientOptions(
            api_endpoint="{}-dataproc.googleapis.com:443".format(self.credential.dataproc_region)
        )
        self.job_client = self._get_job_client()

    def _upload_to_gcs(self, filename: str, compiled_code: str) -> None:
        bucket = self.storage_client.get_bucket(self.credential.gcs_bucket)
        blob = bucket.blob(filename)
        blob.upload_from_string(compiled_code)

    def submit(self, compiled_code: str) -> dataproc_v1.types.jobs.Job:
        # upload python file to GCS
        self._upload_to_gcs(self.model_file_name, compiled_code)
        # submit dataproc job
        return self._submit_dataproc_job()

    def _get_job_client(
        self,
    ) -> Union[dataproc_v1.JobControllerClient, dataproc_v1.BatchControllerClient]:
        raise NotImplementedError("_get_job_client not implemented")

    def _submit_dataproc_job(self) -> dataproc_v1.types.jobs.Job:
        raise NotImplementedError("_submit_dataproc_job not implemented")


class ClusterDataprocHelper(BaseDataProcHelper):
    def _get_job_client(self) -> dataproc_v1.JobControllerClient:
        if not self._get_cluster_name():
            raise ValueError(
                "Need to supply dataproc_cluster_name in profile or config to submit python job with cluster submission method"
            )
        return dataproc_v1.JobControllerClient(  # type: ignore
            client_options=self.client_options, credentials=self.GoogleCredentials
        )

    def _get_cluster_name(self) -> str:
        return self.parsed_model["config"].get(
            "dataproc_cluster_name", self.credential.dataproc_cluster_name
        )

    def _submit_dataproc_job(self) -> dataproc_v1.types.jobs.Job:
        job = {
            "placement": {"cluster_name": self._get_cluster_name()},
            "pyspark_job": {
                "main_python_file_uri": self.gcs_location,
            },
        }
        operation = self.job_client.submit_job_as_operation(  # type: ignore
            request={
                "project_id": self.credential.execution_project,
                "region": self.credential.dataproc_region,
                "job": job,
            }
        )
        response = operation.result(polling=self.result_polling_policy)
        # check if job failed
        if response.status.state == 6:
            raise ValueError(response.status.details)
        return response


class ServerlessDataProcHelper(BaseDataProcHelper):
    def _get_job_client(self) -> dataproc_v1.BatchControllerClient:
        return dataproc_v1.BatchControllerClient(
            client_options=self.client_options, credentials=self.GoogleCredentials
        )

    def _submit_dataproc_job(self) -> dataproc_v1.types.jobs.Job:
        batch = self._configure_batch()
        parent = f"projects/{self.credential.execution_project}/locations/{self.credential.dataproc_region}"

        request = dataproc_v1.CreateBatchRequest(
            parent=parent,
            batch=batch,
        )
        # make the request
        operation = self.job_client.create_batch(request=request)  # type: ignore
        # this takes quite a while, waiting on GCP response to resolve
        # (not a google-api-core issue, more likely a dataproc serverless issue)
        response = operation.result(polling=self.result_polling_policy)
        return response
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

    def _configure_batch(self):
        # create the Dataproc Serverless job config
        # need to pin dataproc version to 1.1 as it now defaults to 2.0
        # https://cloud.google.com/dataproc-serverless/docs/concepts/properties
        # https://cloud.google.com/dataproc-serverless/docs/reference/rest/v1/projects.locations.batches#runtimeconfig
        batch = dataproc_v1.Batch(
            {
                "runtime_config": dataproc_v1.RuntimeConfig(
                    version="1.1",
                    properties={
                        "spark.executor.instances": "2",
                    },
                )
            }
        )
        # Apply defaults
        batch.pyspark_batch.main_python_file_uri = self.gcs_location
        jar_file_uri = self.parsed_model["config"].get(
            "jar_file_uri",
            "gs://spark-lib/bigquery/spark-bigquery-with-dependencies_2.12-0.21.1.jar",
        )
        batch.pyspark_batch.jar_file_uris = [jar_file_uri]

        # Apply configuration from dataproc_batch key, possibly overriding defaults.
        if self.credential.dataproc_batch:
            self._update_batch_from_config(self.credential.dataproc_batch, batch)
        return batch

    @classmethod
    def _update_batch_from_config(
        cls, config_dict: Union[Dict, DataprocBatchConfig], target: dataproc_v1.Batch
    ):
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
