from unittest.mock import ANY, MagicMock

from google.cloud.bigquery import Client, DEFAULT_RETRY, WriteDisposition
import pytest

from dbt.adapters.bigquery.relation import BigQueryRelation
from dbt.adapters.bigquery.services import bigquery


@pytest.mark.parametrize(
    "mode,write_disposition",
    [
        ("incremental", WriteDisposition.WRITE_APPEND),
        ("table", WriteDisposition.WRITE_TRUNCATE),
    ],
)
def test_copy_table(mode: str, write_disposition: WriteDisposition) -> None:
    mock_client = MagicMock(Client)
    source = BigQueryRelation.create("project", "dataset", "table1")
    destination = BigQueryRelation.create("project", "dataset", "table2")

    bigquery.copy_table(mock_client, source, destination, mode)

    mock_client.copy_table.assert_called_once_with(
        [bigquery.table_ref(source)], bigquery.table_ref(destination), job_config=ANY
    )
    _, kwargs = mock_client.copy_table.call_args
    assert kwargs["job_config"].write_disposition == write_disposition


def test_drop_schema():
    mock_client = MagicMock(Client)
    schema = BigQueryRelation.create("db", "schema")
    retry = DEFAULT_RETRY.with_timeout(42)

    bigquery.drop_schema(mock_client, schema, retry)

    mock_client.delete_dataset.assert_called_once_with(
        bigquery.dataset_ref(schema), delete_contents=True, not_found_ok=True, retry=retry
    )


def test_list_schemas():
    mock_client = MagicMock(Client)
    database = "db"
    retry = DEFAULT_RETRY.with_timeout(42)

    bigquery.list_schemas(mock_client, database, retry)

    mock_client.list_datasets.assert_called_once_with(database, max_results=10000, retry=retry)
