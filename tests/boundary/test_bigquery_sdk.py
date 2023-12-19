import pytest

from dbt.tests.util import get_connection
from google.cloud.bigquery import Client, DatasetReference, TableReference
from google.api_core.exceptions import NotFound


class TestBigQuerySDK:
    """
    TODO: replace dbt project methods with direct connection instantiation
    """

    @pytest.mark.parametrize("table_name", ["this_table_does_not_exist"])
    def test_get_table_does_not_exist(self, project, table_name):
        with get_connection(project.adapter) as conn:
            client: Client = conn.handle
            dataset_ref = DatasetReference(project.database, project.test_schema)
            table_ref = TableReference(dataset_ref, table_name)
            with pytest.raises(NotFound):
                client.get_table(table_ref)
