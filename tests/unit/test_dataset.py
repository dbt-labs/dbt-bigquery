from time import sleep
import pytest
from unittest.mock import MagicMock

from dbt.adapters.bigquery.dataset import update_dataset
from dbt.adapters.bigquery import dataset

@pytest.fixture
def mock_client():
    return MagicMock()

@pytest.fixture
def mock_dataset():
    return MagicMock()


def test_update_dataset_calls_bq_client_correctly(mock_client, mock_dataset):
    update_request = (mock_dataset, ["access_entries"])
    update_dataset(mock_client, update_request)
    sleep(1)
    mock_client.update_dataset.assert_called_once_with(*update_request)
    update_dataset(mock_client, update_request)
    sleep(1)
    assert mock_client.update_dataset.call_count == 1


def test_update_dataset_handles_bq_client_error(mock_client, mock_dataset, monkeypatch):
    expected_error = ValueError("something went wrong")
    mock_client.update_dataset.side_effect = expected_error
    #we need to overwrite the updater's client with our new mock
    dataset._dataset_update_thread_store["thread"].client = mock_client
    update_request = (mock_dataset, ["access_entries"])
    mock_logger = MagicMock()
    monkeypatch.setattr(dataset, "logger", mock_logger)
    update_dataset(mock_client, update_request)
    update_dataset(mock_client, update_request)
    sleep(1)
    mock_logger.error.assert_called_with(expected_error)
    assert mock_logger.error.call_count == 2
    
    
def test_update_dataset_reuses_client(mock_client, mock_dataset):
    mock_client_2 = MagicMock()
    update_request = (mock_dataset, ["access_entries"])
    update_dataset(mock_client, update_request)
    update_dataset(mock_client_2, update_request)
    sleep(1)
    mock_client_2.update_dataset.assert_not_called()
