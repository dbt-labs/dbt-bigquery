from google.cloud.bigquery import AccessEntry, Dataset


def is_access_entry_in_dataset(dataset: Dataset, access_entry: AccessEntry) -> bool:
    """Check if the access entry already exists in the dataset.

    Args:
        dataset (Dataset): the dataset to be updated
        access_entry (AccessEntry): the access entry to be added to the dataset

    Returns:
        bool: True if entry exists in dataset, False otherwise
    """
    # we can't simply check if an access entry is in the list
    # the current equality check does not work because the locally created AccessEntry can have extra properties
    for existing_entry in dataset.access_entries:
        if any(
            (
                existing_entry.role == access_entry.role,
                existing_entry.entity_type == access_entry.entity_type,
                existing_entry._properties.items() <= access_entry._properties.items(),
            )
        ):
            return True
    return False
