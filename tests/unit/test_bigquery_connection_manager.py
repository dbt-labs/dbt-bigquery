import time
import json
import pytest
import unittest
from contextlib import contextmanager
from requests.exceptions import ConnectionError
from unittest.mock import patch, MagicMock, Mock, ANY

import dbt.dataclass_schema

from dbt.adapters.bigquery import BigQueryCredentials
from dbt.adapters.bigquery import BigQueryRelation
from dbt.adapters.bigquery.connections import BigQueryConnectionManager
import dbt.exceptions
from dbt.logger import GLOBAL_LOGGER as logger  # noqa


class TestBigQueryConnectionManager(unittest.TestCase):
    def setUp(self):
        credentials = Mock(BigQueryCredentials)
        profile = Mock(query_comment=None, credentials=credentials)
        self.connections = BigQueryConnectionManager(profile=profile)

        self.mock_client = Mock(dbt.adapters.bigquery.impl.google.cloud.bigquery.Client)
        self.mock_connection = MagicMock()

        self.mock_connection.handle = self.mock_client

        self.connections.get_thread_connection = lambda: self.mock_connection
        self.connections.get_job_retry_deadline_seconds = lambda x: None
        self.connections.get_job_retries = lambda x: 1

    @patch("dbt.adapters.bigquery.connections._is_retryable", return_value=True)
    def test_retry_and_handle(self, is_retryable):
        self.connections.DEFAULT_MAXIMUM_DELAY = 2.0

        @contextmanager
        def dummy_handler(msg):
            yield

        self.connections.exception_handler = dummy_handler

        class DummyException(Exception):
            """Count how many times this exception is raised"""

            count = 0

            def __init__(self):
                DummyException.count += 1

        def raiseDummyException():
            raise DummyException()

        with self.assertRaises(DummyException):
            self.connections._retry_and_handle(
                "some sql", Mock(credentials=Mock(retries=8)), raiseDummyException
            )
            self.assertEqual(DummyException.count, 9)

    @patch("dbt.adapters.bigquery.connections._is_retryable", return_value=True)
    def test_retry_connection_reset(self, is_retryable):
        self.connections.open = MagicMock()
        self.connections.close = MagicMock()
        self.connections.DEFAULT_MAXIMUM_DELAY = 2.0

        @contextmanager
        def dummy_handler(msg):
            yield

        self.connections.exception_handler = dummy_handler

        def raiseConnectionResetError():
            raise ConnectionResetError("Connection broke")

        mock_conn = Mock(credentials=Mock(retries=1))
        with self.assertRaises(ConnectionResetError):
            self.connections._retry_and_handle("some sql", mock_conn, raiseConnectionResetError)
        self.connections.close.assert_called_once_with(mock_conn)
        self.connections.open.assert_called_once_with(mock_conn)

    def test_is_retryable(self):
        _is_retryable = dbt.adapters.bigquery.connections._is_retryable
        exceptions = dbt.adapters.bigquery.impl.google.cloud.exceptions
        internal_server_error = exceptions.InternalServerError("code broke")
        bad_request_error = exceptions.BadRequest("code broke")
        connection_error = ConnectionError("code broke")
        client_error = exceptions.ClientError("bad code")
        rate_limit_error = exceptions.Forbidden(
            "code broke", errors=[{"reason": "rateLimitExceeded"}]
        )

        self.assertTrue(_is_retryable(internal_server_error))
        self.assertTrue(_is_retryable(bad_request_error))
        self.assertTrue(_is_retryable(connection_error))
        self.assertFalse(_is_retryable(client_error))
        self.assertTrue(_is_retryable(rate_limit_error))

    def test_drop_dataset(self):
        mock_table = Mock()
        mock_table.reference = "table1"
        self.mock_client.list_tables.return_value = [mock_table]

        self.connections.drop_dataset("project", "dataset")

        self.mock_client.list_tables.assert_not_called()
        self.mock_client.delete_table.assert_not_called()
        self.mock_client.delete_dataset.assert_called_once()

    @patch("dbt.adapters.bigquery.impl.google.cloud.bigquery")
    def test_query_and_results(self, mock_bq):
        self.mock_client.query = Mock(return_value=Mock(state="DONE"))
        self.connections._query_and_results(
            self.mock_client,
            "sql",
            {"job_param_1": "blah"},
            job_creation_timeout=15,
            job_execution_timeout=3,
        )

        mock_bq.QueryJobConfig.assert_called_once()
        self.mock_client.query.assert_called_once_with(
            query="sql", job_config=mock_bq.QueryJobConfig(), timeout=15
        )

    @patch("dbt.adapters.bigquery.impl.google.cloud.bigquery")
    def test_query_and_results_timeout(self, mock_bq):
        self.mock_client.query = Mock(
            return_value=Mock(result=lambda *args, **kwargs: time.sleep(4))
        )
        with pytest.raises(dbt.exceptions.DbtRuntimeError) as exc:
            self.connections._query_and_results(
                self.mock_client,
                "sql",
                {"job_param_1": "blah"},
                job_creation_timeout=15,
                job_execution_timeout=1,
            )

        mock_bq.QueryJobConfig.assert_called_once()
        self.mock_client.query.assert_called_once_with(
            query="sql", job_config=mock_bq.QueryJobConfig(), timeout=15
        )
        assert "Query exceeded configured timeout of 1s" in str(exc.value)

    def test_copy_bq_table_appends(self):
        self._copy_table(write_disposition=dbt.adapters.bigquery.impl.WRITE_APPEND)
        args, kwargs = self.mock_client.copy_table.call_args
        self.mock_client.copy_table.assert_called_once_with(
            [self._table_ref("project", "dataset", "table1")],
            self._table_ref("project", "dataset", "table2"),
            job_config=ANY,
        )
        args, kwargs = self.mock_client.copy_table.call_args
        self.assertEqual(
            kwargs["job_config"].write_disposition, dbt.adapters.bigquery.impl.WRITE_APPEND
        )

    def test_copy_bq_table_truncates(self):
        self._copy_table(write_disposition=dbt.adapters.bigquery.impl.WRITE_TRUNCATE)
        args, kwargs = self.mock_client.copy_table.call_args
        self.mock_client.copy_table.assert_called_once_with(
            [self._table_ref("project", "dataset", "table1")],
            self._table_ref("project", "dataset", "table2"),
            job_config=ANY,
        )
        args, kwargs = self.mock_client.copy_table.call_args
        self.assertEqual(
            kwargs["job_config"].write_disposition, dbt.adapters.bigquery.impl.WRITE_TRUNCATE
        )

    def test_job_labels_valid_json(self):
        expected = {"key": "value"}
        labels = self.connections._labels_from_query_comment(json.dumps(expected))
        self.assertEqual(labels, expected)

    def test_job_labels_invalid_json(self):
        labels = self.connections._labels_from_query_comment("not json")
        self.assertEqual(labels, {"query_comment": "not_json"})

    def test_list_dataset_correctly_calls_lists_datasets(self):
        mock_dataset = Mock(dataset_id="d1")
        mock_list_dataset = Mock(return_value=[mock_dataset])
        self.mock_client.list_datasets = mock_list_dataset
        result = self.connections.list_dataset("project")
        self.mock_client.list_datasets.assert_called_once_with(
            project="project", max_results=10000
        )
        assert result == ["d1"]

    def _table_ref(self, proj, ds, table):
        return self.connections.table_ref(proj, ds, table)

    def _copy_table(self, write_disposition):
        source = BigQueryRelation.create(database="project", schema="dataset", identifier="table1")
        destination = BigQueryRelation.create(
            database="project", schema="dataset", identifier="table2"
        )
        self.connections.copy_bq_table(source, destination, write_disposition)
