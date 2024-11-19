from google.api_core.client_info import ClientInfo
from google.api_core.client_options import ClientOptions
from google.api_core.retry import Retry
from google.auth.exceptions import DefaultCredentialsError
from google.cloud.bigquery import Client as BigQueryClient
from google.cloud.dataproc_v1 import BatchControllerClient, JobControllerClient
from google.cloud.storage import Client as StorageClient

from dbt.adapters.events.logging import AdapterLogger

import dbt.adapters.bigquery.__version__ as dbt_version
from dbt.adapters.bigquery.credentials import (
    BigQueryCredentials,
    create_google_credentials,
    set_default_credentials,
)


_logger = AdapterLogger("BigQuery")


def create_bigquery_client(credentials: BigQueryCredentials) -> BigQueryClient:
    try:
        return _create_bigquery_client(credentials)
    except DefaultCredentialsError:
        _logger.info("Please log into GCP to continue")
        set_default_credentials()
        return _create_bigquery_client(credentials)


@Retry()  # google decorator. retries on transient errors with exponential backoff
def create_gcs_client(credentials: BigQueryCredentials) -> StorageClient:
    return StorageClient(
        project=credentials.execution_project,
        credentials=create_google_credentials(credentials),
    )


@Retry()  # google decorator. retries on transient errors with exponential backoff
def create_dataproc_job_controller_client(credentials: BigQueryCredentials) -> JobControllerClient:
    return JobControllerClient(
        credentials=create_google_credentials(credentials),
        client_options=ClientOptions(api_endpoint=_dataproc_endpoint(credentials)),
    )


@Retry()  # google decorator. retries on transient errors with exponential backoff
def create_dataproc_batch_controller_client(
    credentials: BigQueryCredentials,
) -> BatchControllerClient:
    return BatchControllerClient(
        credentials=create_google_credentials(credentials),
        client_options=ClientOptions(api_endpoint=_dataproc_endpoint(credentials)),
    )


@Retry()  # google decorator. retries on transient errors with exponential backoff
def _create_bigquery_client(credentials: BigQueryCredentials) -> BigQueryClient:
    return BigQueryClient(
        credentials.execution_project,
        create_google_credentials(credentials),
        location=getattr(credentials, "location", None),
        client_info=ClientInfo(user_agent=f"dbt-bigquery-{dbt_version.version}"),
        client_options=ClientOptions(quota_project_id=credentials.quota_project),
    )


def _dataproc_endpoint(credentials: BigQueryCredentials) -> str:
    return f"{credentials.dataproc_region}-dataproc.googleapis.com:443"
