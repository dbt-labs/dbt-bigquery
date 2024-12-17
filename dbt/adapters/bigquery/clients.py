from google.api_core.client_info import ClientInfo
from google.api_core.client_options import ClientOptions
from google.auth.exceptions import DefaultCredentialsError
from google.cloud import bigquery, dataproc_v1, storage

from dbt.adapters.events.logging import AdapterLogger

import dbt.adapters.bigquery.__version__ as dbt_version
from dbt.adapters.bigquery.credentials import (
    BigQueryCredentials,
    create_google_credentials,
    set_default_credentials,
)


_logger = AdapterLogger("BigQuery")


def create_bigquery_client(credentials: BigQueryCredentials) -> bigquery.Client:
    try:
        return _create_bigquery_client(credentials)
    except DefaultCredentialsError:
        _logger.info("Please log into GCP to continue")
        set_default_credentials()
        return _create_bigquery_client(credentials)


@storage.DEFAULT_RETRY
def create_gcs_client(credentials: BigQueryCredentials) -> storage.Client:
    return storage.Client(
        project=credentials.execution_project,
        credentials=create_google_credentials(credentials),
    )


@dataproc_v1.DEFAULT_RETRY
def create_dataproc_job_controller_client(
    credentials: BigQueryCredentials,
) -> dataproc_v1.JobControllerClient:
    return dataproc_v1.JobControllerClient(
        credentials=create_google_credentials(credentials),
        client_options=ClientOptions(api_endpoint=_dataproc_endpoint(credentials)),
    )


@dataproc_v1.DEFAULT_RETRY
def create_dataproc_batch_controller_client(
    credentials: BigQueryCredentials,
) -> dataproc_v1.BatchControllerClient:
    return dataproc_v1.BatchControllerClient(
        credentials=create_google_credentials(credentials),
        client_options=ClientOptions(api_endpoint=_dataproc_endpoint(credentials)),
    )


@bigquery.DEFAULT_RETRY
def _create_bigquery_client(credentials: BigQueryCredentials) -> bigquery.Client:
    return bigquery.Client(
        credentials.execution_project,
        create_google_credentials(credentials),
        location=getattr(credentials, "location", None),
        client_info=ClientInfo(user_agent=f"dbt-bigquery-{dbt_version.version}"),
        client_options=ClientOptions(quota_project_id=credentials.quota_project),
    )


def _dataproc_endpoint(credentials: BigQueryCredentials) -> str:
    return f"{credentials.dataproc_region}-dataproc.googleapis.com:443"
