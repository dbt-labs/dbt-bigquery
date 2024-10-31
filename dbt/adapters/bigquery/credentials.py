from dataclasses import dataclass, field
from functools import lru_cache
from typing import Any, Dict, Optional, Tuple

import google.auth
from google.auth.exceptions import DefaultCredentialsError
from mashumaro import pass_through

from dbt_common.dataclass_schema import ExtensibleDbtClassMixin, StrEnum
from dbt_common.exceptions import DbtConfigError, DbtRuntimeError
from dbt.adapters.contracts.connection import Credentials


class Priority(StrEnum):
    Interactive = "interactive"
    Batch = "batch"


class BigQueryConnectionMethod(StrEnum):
    OAUTH = "oauth"
    SERVICE_ACCOUNT = "service-account"
    SERVICE_ACCOUNT_JSON = "service-account-json"
    OAUTH_SECRETS = "oauth-secrets"


@dataclass
class DataprocBatchConfig(ExtensibleDbtClassMixin):
    def __init__(self, batch_config):
        self.batch_config = batch_config


@lru_cache()
def get_bigquery_defaults(scopes=None) -> Tuple[Any, Optional[str]]:
    """
    Returns (credentials, project_id)

    project_id is returned available from the environment; otherwise None
    """
    # Cached, because the underlying implementation shells out, taking ~1s
    try:
        credentials, _ = google.auth.default(scopes=scopes)
        return credentials, _
    except DefaultCredentialsError as e:
        raise DbtConfigError(f"Failed to authenticate with supplied credentials\nerror:\n{e}")


@dataclass
class BigQueryCredentials(Credentials):
    method: BigQueryConnectionMethod = None  # type: ignore

    # BigQuery allows an empty database / project, where it defers to the
    # environment for the project
    database: Optional[str] = None
    schema: Optional[str] = None
    execution_project: Optional[str] = None
    location: Optional[str] = None
    priority: Optional[Priority] = None
    maximum_bytes_billed: Optional[int] = None
    impersonate_service_account: Optional[str] = None

    job_retry_deadline_seconds: Optional[int] = None
    job_retries: Optional[int] = 1
    job_creation_timeout_seconds: Optional[int] = None
    job_execution_timeout_seconds: Optional[int] = None

    # Keyfile json creds (unicode or base 64 encoded)
    keyfile: Optional[str] = None
    keyfile_json: Optional[Dict[str, Any]] = None

    # oauth-secrets
    token: Optional[str] = None
    refresh_token: Optional[str] = None
    client_id: Optional[str] = None
    client_secret: Optional[str] = None
    token_uri: Optional[str] = None

    dataproc_region: Optional[str] = None
    dataproc_cluster_name: Optional[str] = None
    gcs_bucket: Optional[str] = None

    dataproc_batch: Optional[DataprocBatchConfig] = field(
        metadata={
            "serialization_strategy": pass_through,
        },
        default=None,
    )

    scopes: Optional[Tuple[str, ...]] = (
        "https://www.googleapis.com/auth/bigquery",
        "https://www.googleapis.com/auth/cloud-platform",
        "https://www.googleapis.com/auth/drive",
    )

    _ALIASES = {
        # 'legacy_name': 'current_name'
        "project": "database",
        "dataset": "schema",
        "target_project": "target_database",
        "target_dataset": "target_schema",
        "retries": "job_retries",
        "timeout_seconds": "job_execution_timeout_seconds",
    }

    def __post_init__(self):
        if self.keyfile_json and "private_key" in self.keyfile_json:
            self.keyfile_json["private_key"] = self.keyfile_json["private_key"].replace(
                "\\n", "\n"
            )
        if not self.method:
            raise DbtRuntimeError("Must specify authentication method")

        if not self.schema:
            raise DbtRuntimeError("Must specify schema")

    @property
    def type(self):
        return "bigquery"

    @property
    def unique_field(self):
        return self.database

    def _connection_keys(self):
        return (
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
            "timeout_seconds",
            "client_id",
            "token_uri",
            "dataproc_region",
            "dataproc_cluster_name",
            "gcs_bucket",
            "dataproc_batch",
        )

    @classmethod
    def __pre_deserialize__(cls, d: Dict[Any, Any]) -> Dict[Any, Any]:
        # We need to inject the correct value of the database (aka project) at
        # this stage, ref
        # https://github.com/dbt-labs/dbt/pull/2908#discussion_r532927436.

        # `database` is an alias of `project` in BigQuery
        if "database" not in d:
            _, database = get_bigquery_defaults()
            d["database"] = database
        # `execution_project` default to dataset/project
        if "execution_project" not in d:
            d["execution_project"] = d["database"]
        return d
