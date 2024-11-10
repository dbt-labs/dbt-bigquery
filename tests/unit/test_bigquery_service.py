from unittest.mock import ANY, MagicMock

from google.cloud.bigquery import Client, WriteDisposition
import pytest

from dbt.adapters.bigquery.relation import BigQueryRelation
from dbt.adapters.bigquery.services import BigQueryService, table_ref


@pytest.mark.parametrize(
    "mode,write_disposition",
    [
        ("incremental", WriteDisposition.WRITE_APPEND),
        ("table", WriteDisposition.WRITE_TRUNCATE),
    ],
)
def test_copy_table_incremental(mode: str, write_disposition: WriteDisposition) -> None:
    mock_client = MagicMock(Client)
    source = BigQueryRelation.create(database="project", schema="dataset", identifier="table1")
    destination = BigQueryRelation.create(
        database="project", schema="dataset", identifier="table2"
    )

    BigQueryService().copy_table(mock_client, source, destination, mode)

    mock_client.copy_table.assert_called_once_with(
        [table_ref(source)], table_ref(destination), job_config=ANY
    )
    _, kwargs = mock_client.copy_table.call_args
    assert kwargs["job_config"].write_disposition == write_disposition
