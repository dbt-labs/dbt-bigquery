from typing import Iterator, List, Optional

from google.api_core.retry import Retry
from google.cloud.bigquery import Client, Dataset, DatasetReference
from google.cloud.exceptions import NotFound

from dbt.adapters.base import BaseRelation

from dbt.adapters.bigquery.relation import BigQueryRelation
from dbt.adapters.bigquery.services.bigquery._config import logger
from dbt.adapters.bigquery.services.bigquery._exception import exception_handler


def list_schemas(client: Client, database: str, retry: Retry) -> List[str]:
    return [dataset.dataset_id for dataset in list_datasets(client, database, retry)]


def list_datasets(client: Client, project: str, retry: Retry) -> Iterator[Dataset]:
    # The database string we get here is potentially quoted.
    # Strip that off for the API call.
    with exception_handler():
        return client.list_datasets(project.strip("`"), max_results=10000, retry=retry)


def get_dataset(client: Client, schema: BigQueryRelation) -> Optional[Dataset]:
    try:
        return client.get_dataset(dataset_ref(schema))
    except NotFound:
        return None


def drop_schema(client: Client, schema: BigQueryRelation, retry: Retry) -> None:
    logger.debug(f'Dropping schema "{schema.database}.{schema.schema}".')

    with exception_handler():
        client.delete_dataset(
            dataset_ref(schema),
            delete_contents=True,
            not_found_ok=True,
            retry=retry,
        )


def schema_exists(client: Client, schema: BigQueryRelation) -> bool:
    """
    Determine whether a dataset exists.

    This tries to do something with the dataset and checks for an exception.
    If the dataset doesn't exist it will 404.
    We have to do it this way to handle underscore-prefixed datasets,
    which don't appear in the information_schema.schemata view nor the
    list_datasets method.

    Args:
        client: a client with view privileges on the dataset
        schema: the dataset that we're checking

    Returns:
        True if the dataset exists, False otherwise
    """
    dataset = dataset_ref(schema)
    try:
        next(iter(client.list_tables(dataset, max_results=1)))
    except StopIteration:
        pass
    except NotFound:
        # the schema does not exist
        return False
    return True


def dataset_ref(schema: BaseRelation) -> DatasetReference:
    return DatasetReference(schema.database, schema.schema)
