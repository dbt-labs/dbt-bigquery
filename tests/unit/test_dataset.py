from dbt.adapters.bigquery.dataset import add_access_entry_to_dataset
from dbt.adapters.bigquery import BigQueryRelation

from google.cloud.bigquery import Dataset, AccessEntry, DatasetReference

def test_add_access_entry_to_dataset_idempotently_adds_entries():
    database = "someDb"
    dataset = "someDataset"
    table = "someTable"
    entity = BigQueryRelation.from_dict(
        {
            "type": None,
            "path": {"database": "test-project", "schema": "test_schema", "identifier": "my_view"},
            "quote_policy": {"identifier": False},
        }
    )
    dataset_ref = DatasetReference(project=database, dataset_id=dataset)
    dataset = Dataset(dataset_ref)
    access_entry = AccessEntry(None, "table", entity)
    dataset = add_access_entry_to_dataset(dataset, access_entry)
    assert access_entry in dataset.access_entries
    dataset = add_access_entry_to_dataset(dataset, access_entry)
    assert len(dataset.access_entries) == 1
