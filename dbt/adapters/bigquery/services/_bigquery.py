from contextlib import contextmanager
from typing import Generator, List, Optional, Union

from google.auth.exceptions import RefreshError
from google.cloud.bigquery import (
    Client,
    CopyJob,
    CopyJobConfig,
    DatasetReference,
    TableReference,
    WriteDisposition,
    QueryJob,
)
from google.cloud.exceptions import BadRequest, ClientError, Forbidden, NotFound

from dbt_common.exceptions import DbtDatabaseError, DbtRuntimeError
from dbt.adapters.events.logging import AdapterLogger

from dbt.adapters.bigquery.relation import BigQueryRelation


_logger = AdapterLogger("BigQuery")


class BigQueryService:
    def __init__(self) -> None:
        pass

    def copy_table(
        self,
        client: Client,
        sources: Union[BigQueryRelation, List[BigQueryRelation]],
        destination: BigQueryRelation,
        mode: str,
        timeout: Optional[float] = 300,
    ) -> CopyJob:
        # -------------------------------------------------------------------------------
        #  BigQuery allows to use copy API using two different formats:
        #  1. client.copy_table(source_table_id, destination_table_id)
        #     where source_table_id = "your-project.source_dataset.source_table"
        #  2. client.copy_table(source_table_ids, destination_table_id)
        #     where source_table_ids = ["your-project.your_dataset.your_table_name", ...]
        #  Let's use uniform function call and always pass list there
        # -------------------------------------------------------------------------------
        if isinstance(sources, list):
            source_refs = [table_ref(src_table) for src_table in sources]
        else:
            source_refs = [table_ref(sources)]
        destination_ref = table_ref(destination)

        write_disposition = {
            "incremental": WriteDisposition.WRITE_APPEND,
            "table": WriteDisposition.WRITE_TRUNCATE,
        }.get(mode, WriteDisposition.WRITE_TRUNCATE)

        _logger.debug(
            'Copying table(s) "{}" to "{}" with disposition: "{}"',
            ", ".join(source_ref.path for source_ref in source_refs),
            destination_ref.path,
            write_disposition,
        )

        with self._exception_handler():
            config = CopyJobConfig(write_disposition=write_disposition)
            copy_job = client.copy_table(source_refs, destination_ref, job_config=config)
            return copy_job.result(timeout=timeout)

    @contextmanager
    def _exception_handler(self) -> Generator:
        try:
            yield
        except BadRequest as e:
            self._database_error(e, "Bad request while running query")
        except Forbidden as e:
            self._database_error(e, "Access denied while running query")
        except NotFound as e:
            self._database_error(e, "Not found while running query")
        except RefreshError as e:
            message = (
                "Unable to generate access token, if you're using "
                "impersonate_service_account, make sure your "
                'initial account has the "roles/'
                'iam.serviceAccountTokenCreator" role on the '
                "account you are trying to impersonate.\n\n"
                f"{str(e)}"
            )
            raise DbtRuntimeError(message)

    @classmethod
    def _database_error(cls, error: ClientError, message: str) -> None:
        if hasattr(error, "query_job"):
            _logger.error(query_job_url(error.query_job))
        msg = message
        msg += "\n".join([item["message"] for item in error.errors])
        raise DbtDatabaseError(msg)


def table_ref(relation: BigQueryRelation) -> TableReference:
    return TableReference(dataset_ref(relation), relation.identifier)


def dataset_ref(relation: BigQueryRelation) -> DatasetReference:
    return DatasetReference(relation.database, relation.schema)


def query_job_url(query_job: QueryJob) -> str:
    return f"https://console.cloud.google.com/bigquery?project={query_job.project}&j=bq:{query_job.location}:{query_job.job_id}&page=queryresults"
