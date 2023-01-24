from typing import List
from google.cloud.bigquery import Dataset, AccessEntry

from dbt.events import AdapterLogger

logger = AdapterLogger("BigQuery")


def add_access_entry_to_dataset(dataset: Dataset, access_entry: AccessEntry) -> Dataset:
    """Idempotently adds an access entry to a dataset

    Args:
        dataset (Dataset): the dataset to be updated
        access_entry (AccessEntry): the access entry to be added to the dataset

    Returns:
        Dataset
    """
    access_entries: List[AccessEntry] = dataset.access_entries
    # we can't simply check if an access entry is in the list as the current equality check
    # does not work because the locally created AccessEntry can have extra properties.
    for existing_entry in access_entries:
        if access_entry._properties.items() <= existing_entry._properties.items():
            logger.warning(f"Access entry {access_entry} " f"already exists in dataset")
            return dataset
    access_entries.append(access_entry)
    dataset.access_entries = access_entries
    return dataset
