import json
import unittest
from requests.exceptions import ConnectionError
from unittest.mock import patch, MagicMock, Mock, ANY

import dbt.adapters
import google.cloud.bigquery

from dbt.adapters.bigquery import BigQueryCredentials
from dbt.adapters.bigquery.connections import BigQueryConnectionManager
from dbt.adapters.bigquery.retry import RetryFactory


class TestBigQueryConnectionManager(unittest.TestCase):
    def setUp(self):
        self.credentials = Mock(BigQueryCredentials)
        self.credentials.method = "oauth"
        self.credentials.job_retries = 1
        self.credentials.job_retry_deadline_seconds = 1
        self.credentials.scopes = tuple()

        self.mock_client = Mock(google.cloud.bigquery.Client)

        self.mock_connection = MagicMock()
        self.mock_connection.handle = self.mock_client
        self.mock_connection.credentials = self.credentials

        self.connections = BigQueryConnectionManager(
            profile=Mock(credentials=self.credentials, query_comment=None),
            mp_context=Mock(),
        )
        self.connections.get_thread_connection = lambda: self.mock_connection

    @patch(
        "dbt.adapters.bigquery.retry.bigquery_client",
        return_value=Mock(google.cloud.bigquery.Client),
    )
    def test_retry_connection_reset(self, mock_client_factory):
        new_mock_client = mock_client_factory.return_value

        @self.connections._retry.reopen_with_deadline(self.mock_connection)
        def generate_connection_reset_error():
            raise ConnectionResetError

        assert self.mock_connection.handle is self.mock_client

        with self.assertRaises(ConnectionResetError):
            # this will always raise the error, we just want to test that the connection was reopening in between
            generate_connection_reset_error()

        assert self.mock_connection.handle is new_mock_client
        assert new_mock_client is not self.mock_client

    def test_is_retryable(self):
        _is_retryable = dbt.adapters.bigquery.retry._is_retryable
        exceptions = dbt.adapters.bigquery.impl.google.cloud.exceptions
        internal_server_error = exceptions.InternalServerError("code broke")
        bad_request_error = exceptions.BadRequest("code broke")
        connection_error = ConnectionError("code broke")
        client_error = exceptions.ClientError("bad code")
        rate_limit_error = exceptions.Forbidden(
            "code broke", errors=[{"reason": "rateLimitExceeded"}]
        )
        service_unavailable_error = exceptions.ServiceUnavailable("service is unavailable")

        self.assertTrue(_is_retryable(internal_server_error))
        self.assertTrue(_is_retryable(bad_request_error))
        self.assertTrue(_is_retryable(connection_error))
        self.assertFalse(_is_retryable(client_error))
        self.assertTrue(_is_retryable(rate_limit_error))
        self.assertTrue(_is_retryable(service_unavailable_error))

    @patch("dbt.adapters.bigquery.connections.QueryJobConfig")
    def test_query_and_results(self, MockQueryJobConfig):
        self.connections._query_and_results(
            self.mock_connection,
            "sql",
            {"dry_run": True},
            job_id=1,
        )

        MockQueryJobConfig.assert_called_once()
        self.mock_client.query.assert_called_once_with(
            query="sql",
            job_config=MockQueryJobConfig(),
            job_id=1,
            timeout=self.credentials.job_creation_timeout_seconds,
        )

    def test_job_labels_valid_json(self):
        expected = {"key": "value"}
        labels = self.connections._labels_from_query_comment(json.dumps(expected))
        self.assertEqual(labels, expected)

    def test_job_labels_invalid_json(self):
        labels = self.connections._labels_from_query_comment("not json")
        self.assertEqual(labels, {"query_comment": "not_json"})

    def _table_ref(self, proj, ds, table):
        return self.connections.table_ref(proj, ds, table)
