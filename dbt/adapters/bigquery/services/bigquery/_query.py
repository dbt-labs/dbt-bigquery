from dataclasses import dataclass
from typing import Any, Dict, Optional, Tuple

from google.cloud.bigquery import Client, QueryJob, QueryJobConfig
from google.cloud.bigquery.table import RowIterator

from dbt.adapters.contracts.connection import AdapterResponse

from dbt.adapters.bigquery.services.bigquery._config import DEFAULT_TIMEOUT, logger, query_job_url


@dataclass
class BigQueryAdapterResponse(AdapterResponse):
    bytes_processed: Optional[int] = None
    bytes_billed: Optional[int] = None
    location: Optional[str] = None
    project_id: Optional[str] = None
    job_id: Optional[str] = None
    slot_ms: Optional[int] = None


def execute(
    client: Client,
    sql: str,
    config: Dict[str, Any],
    create_timeout: Optional[float] = DEFAULT_TIMEOUT,
    execute_timeout: Optional[float] = DEFAULT_TIMEOUT,
    job_id: Optional[str] = None,
    limit: Optional[int] = None,
) -> Tuple[QueryJob, RowIterator]:
    """
    Query the client and return the results.

    Args:
        client:
        sql:
        config:
        create_timeout:
        execute_timeout:
        job_id:
        limit:

    Returns:
        The query job itself and a row iterator containing the results
    """
    # Cannot reuse job_config if destination is set and ddl is used
    query_job = client.query(
        query=sql,
        job_config=QueryJobConfig(**config),
        job_id=job_id,  # note, this disables retry since the job_id will have been used
        timeout=create_timeout,
    )

    if all((query_job.location, query_job.job_id, query_job.project)):
        logger.debug(query_job_url(query_job))

    try:
        iterator = query_job.result(max_results=limit, timeout=execute_timeout)
    except TimeoutError:
        raise TimeoutError(
            f"Operation did not complete within the designated timeout of {execute_timeout} seconds."
        )

    return query_job, iterator


def query_job_response(client: Client, query_job: QueryJob) -> BigQueryAdapterResponse:

    if query_job.dry_run:
        code = "DRY RUN"
    elif query_job.statement_type == "CREATE_TABLE_AS_SELECT":
        code = "CREATE TABLE"
    elif query_job.statement_type == "CREATE_VIEW":
        code = "CREATE VIEW"
    elif query_job.statement_type in ["DELETE", "INSERT", "MERGE", "SCRIPT", "SELECT", "UPDATE"]:
        code = query_job.statement_type
    else:
        code = "OK"

    if query_job.statement_type in ["CREATE_TABLE_AS_SELECT", "SELECT"]:
        query_table = client.get_table(query_job.destination)
        num_rows = query_table.num_rows
    elif query_job.statement_type in ["DELETE", "INSERT", "MERGE", "UPDATE"]:
        num_rows = query_job.num_dml_affected_rows
    else:
        num_rows = 0

    message = f"{code}"
    message += f" {_format_rows_number(num_rows)}"
    message += f" {_format_bytes(query_job.total_bytes_processed)}"

    return BigQueryAdapterResponse(
        _message=message,
        code=code,
        rows_affected=num_rows,
        bytes_processed=query_job.total_bytes_processed,
        bytes_billed=query_job.total_bytes_billed,
        location=query_job.location,
        project_id=query_job.project,
        job_id=query_job.job_id,
        slot_ms=query_job.slot_millis,
    )


def _format_bytes(raw_bytes: int) -> str:
    if raw_bytes:
        for unit in ["Bytes", "KiB", "MiB", "GiB", "TiB", "PiB"]:
            if abs(raw_bytes) < 1024.0:
                return f"{raw_bytes:3.1f} {unit}"
            raw_bytes /= 1024.0  # type: ignore

        raw_bytes *= 1024.0  # type: ignore
        return f"{raw_bytes:3.1f} {unit} processed"
    return f"{raw_bytes} processed"


def _format_rows_number(raw_rows: int) -> str:
    for unit in ["", "k", "m", "b", "t"]:
        if abs(raw_rows) < 1000.0:
            return f"{raw_rows:3.1f}{unit} rows".strip()
        raw_rows /= 1000.0  # type: ignore

    raw_rows *= 1000.0  # type: ignore
    return f"{raw_rows:3.1f}{unit} rows".strip()
