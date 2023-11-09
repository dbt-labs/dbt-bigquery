from typing import Union, Dict

import time
from datetime import datetime
from google.cloud.dataproc_v1 import (
    CreateBatchRequest,
    BatchControllerClient,
    Batch,
    GetBatchRequest,
)
from google.protobuf.json_format import ParseDict

from dbt.adapters.bigquery.connections import DataprocBatchConfig

_BATCH_RUNNING_STATES = [Batch.State.PENDING, Batch.State.RUNNING]
DEFAULT_JAR_FILE_URI = "gs://spark-lib/bigquery/spark-bigquery-with-dependencies_2.13-0.34.0.jar"


def create_batch_request(
    batch: Batch, batch_id: str, project: str, region: str
) -> CreateBatchRequest:
    return CreateBatchRequest(
        parent=f"projects/{project}/locations/{region}",  # type: ignore
        batch_id=batch_id,  # type: ignore
        batch=batch,  # type: ignore
    )


def poll_batch_job(
    parent: str, batch_id: str, job_client: BatchControllerClient, timeout: int
) -> Batch:
    batch_name = "".join([parent, "/batches/", batch_id])
    state = Batch.State.PENDING
    response = None
    run_time = 0
    while state in _BATCH_RUNNING_STATES and run_time < timeout:
        time.sleep(1)
        response = job_client.get_batch(  # type: ignore
            request=GetBatchRequest(name=batch_name),  # type: ignore
        )
        run_time = datetime.now().timestamp() - response.create_time.timestamp()  # type: ignore
        state = response.state
    if not response:
        raise ValueError("No response from Dataproc")
    if state != Batch.State.SUCCEEDED:
        if run_time >= timeout:
            raise ValueError(
                f"Operation did not complete within the designated timeout of {timeout} seconds."
            )
        else:
            raise ValueError(response.state_message)
    return response


def update_batch_from_config(config_dict: Union[Dict, DataprocBatchConfig], target: Batch):
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
