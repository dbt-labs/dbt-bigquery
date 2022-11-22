import unittest
from unittest.mock import MagicMock

from dbt.adapters.bigquery.dataset import update_dataset

class TestBigQueryDatasetUpdate(unittest.TestCase):
    mock_client = MagicMock()

    def test_start_dataset_update_thread(self):
        update_dataset(self.mock_client)
        self.mock_client.assert_called_once()