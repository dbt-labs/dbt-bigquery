from typing import List

from google.cloud.bigquery import AccessEntry, Dataset

from dbt.adapters.events.logging import AdapterLogger


logger = AdapterLogger("BigQuery")


def is_access_entry_in_dataset(dataset: Dataset, access_entry: AccessEntry) -> bool:
    """Check if the access entry already exists in the dataset.

    Args:
        dataset (Dataset): the dataset to be updated
        access_entry (AccessEntry): the access entry to be added to the dataset

    Returns:
        bool: True if entry exists in dataset, False otherwise
    """
    access_entries: List[AccessEntry] = dataset.access_entries
    # we can't simply check if an access entry is in the list as the current equality check
    # does not work because the locally created AccessEntry can have extra properties.
    for existing_entry in access_entries:
        role_match = existing_entry.role == access_entry.role
        entity_type_match = existing_entry.entity_type == access_entry.entity_type
        property_match = existing_entry._properties.items() <= access_entry._properties.items()
        if role_match and entity_type_match and property_match:
            return True
    return False


def add_access_entry_to_dataset(dataset: Dataset, access_entry: AccessEntry) -> Dataset:
    """Adds an access entry to a dataset, always use access_entry_present_in_dataset to check
    if the access entry already exists before calling this function.

    Args:
        dataset (Dataset): the dataset to be updated
        access_entry (AccessEntry): the access entry to be added to the dataset

    Returns:
        Dataset: the updated dataset
    """
    access_entries: List[AccessEntry] = dataset.access_entries
    access_entries.append(access_entry)
    dataset.access_entries = access_entries
    return dataset
