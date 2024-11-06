from google.api_core.client_info import ClientInfo
from google.api_core.client_options import ClientOptions
from google.auth.exceptions import DefaultCredentialsError
from google.cloud.bigquery import Client as BigQueryClient
from google.cloud.dataproc_v1 import BatchControllerClient, JobControllerClient
from google.cloud.storage import Client as StorageClient

from dbt.adapters.events.logging import AdapterLogger

import dbt.adapters.bigquery.__version__ as dbt_version
from dbt.adapters.bigquery.credentials import (
    BigQueryCredentials,
    google_credentials,
    setup_default_credentials,
)


_logger = AdapterLogger("BigQuery")


def bigquery_client(credentials: BigQueryCredentials) -> BigQueryClient:
    try:
        return _bigquery_client(credentials)
    except DefaultCredentialsError:
        _logger.info("Please log into GCP to continue")
        setup_default_credentials()
        return _bigquery_client(credentials)


def storage_client(credentials: BigQueryCredentials) -> StorageClient:
    return StorageClient(
        project=credentials.execution_project,
        credentials=google_credentials(credentials),
    )


def job_controller_client(credentials: BigQueryCredentials) -> JobControllerClient:
    options = ClientOptions(
        api_endpoint=f"{credentials.dataproc_region}-dataproc.googleapis.com:443",
    )
    return JobControllerClient(
        credentials=google_credentials(credentials),
        client_options=options,
    )


def batch_controller_client(credentials: BigQueryCredentials) -> BatchControllerClient:
    options = ClientOptions(
        api_endpoint=f"{credentials.dataproc_region}-dataproc.googleapis.com:443",
    )
    return BatchControllerClient(
        credentials=google_credentials(credentials),
        client_options=options,
    )


def _bigquery_client(credentials: BigQueryCredentials) -> BigQueryClient:
    return BigQueryClient(
        credentials.execution_project,
        google_credentials(credentials),
        location=getattr(credentials, "location", None),
        client_info=ClientInfo(user_agent=f"dbt-bigquery-{dbt_version.version}"),
        client_options=ClientOptions(quota_project_id=credentials.quota_project),
    )
