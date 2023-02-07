from dbt.adapters.bigquery.dataset import add_access_entry_to_dataset
from dbt.adapters.bigquery import BigQueryRelation

from google.cloud.bigquery import Dataset, AccessEntry, DatasetReference


def test_add_access_entry_to_dataset_idempotently_adds_entries():
    database = "someDb"
    dataset = "someDataset"
    entity = BigQueryRelation.from_dict(
        {
            "type": None,
            "path": {"database": "test-project", "schema": "test_schema", "identifier": "my_table"},
            "quote_policy": {"identifier": False},
        }
    ).to_dict()
    dataset_ref = DatasetReference(project=database, dataset_id=dataset)
    dataset = Dataset(dataset_ref)
    access_entry = AccessEntry(None, "table", entity)
    dataset = add_access_entry_to_dataset(dataset, access_entry)
    assert access_entry in dataset.access_entries
    dataset = add_access_entry_to_dataset(dataset, access_entry)
    assert len(dataset.access_entries) == 1


def test_add_access_entry_to_dataset_does_not_add_with_pre_existing_entries():
    database = "someOtherDb"
    dataset = "someOtherDataset"
    entity_2 = BigQueryRelation.from_dict(
        {
            "type": None,
            "path": {"database": "test-project", "schema": "test_schema", "identifier": "some_other_view"},
            "quote_policy": {"identifier": False},
        }
    ).to_dict()
    dataset_ref = DatasetReference(project=database, dataset_id=dataset)
    dataset = Dataset(dataset_ref)
    initial_entry = AccessEntry(None, "view", entity_2)
    initial_entry._properties.pop("role")
    dataset.access_entries = [initial_entry]
    access_entry = AccessEntry(None, "view", entity_2)
    dataset = add_access_entry_to_dataset(dataset, access_entry)
    assert len(dataset.access_entries) == 1
