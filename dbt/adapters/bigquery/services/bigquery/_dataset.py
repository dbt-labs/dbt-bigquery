from typing import Any, Dict, List, Optional

from google.api_core.retry import Retry
from google.cloud.bigquery.client import Client
from google.cloud.bigquery.dataset import Dataset, DatasetReference
from google.cloud.bigquery.retry import DEFAULT_RETRY
from google.cloud.exceptions import NotFound

from dbt.adapters.base import BaseRelation

from dbt.adapters.bigquery.relation import BigQueryRelation
from dbt.adapters.bigquery.services._config import logger
from dbt.adapters.bigquery.services.bigquery._exception import exception_handler


class BigQueryDatasetService:
    def __init__(self, client: Client) -> None:
        self._client = client

    def list_schemas(self, database: str, retry: Retry = DEFAULT_RETRY) -> List[BigQueryRelation]:
        return [
            BigQueryRelation.create(database, dataset.dataset_id)
            for dataset in self.list(database, retry)
        ]

    def list(self, project: str, retry: Retry = DEFAULT_RETRY) -> List[Dataset]:
        with exception_handler():
            return list(
                self._client.list_datasets(project.strip("`"), max_results=10000, retry=retry)
            )

    def get(self, schema: BigQueryRelation, retry: Retry = DEFAULT_RETRY) -> Optional[Dataset]:
        try:
            with exception_handler():
                return self._client.get_dataset(self.ref(schema), retry=retry)
        except NotFound:
            return None

    def update(
        self,
        relation: BigQueryRelation,
        updates: Dict[str, Any],
        retry: Optional[Retry] = DEFAULT_RETRY,
    ) -> Dataset:
        dataset = self.get(relation, retry)
        for k, v in updates.items():
            setattr(dataset, k, v)
        self._client.update_dataset(dataset, list(updates.keys()))
        return dataset

    def drop(self, schema: BigQueryRelation, retry: Retry) -> None:
        logger.debug(f'Dropping schema "{schema.database}.{schema.schema}".')

        with exception_handler():
            self._client.delete_dataset(
                self.ref(schema),
                delete_contents=True,
                not_found_ok=True,
                retry=retry,
            )

    def exists(self, schema: BigQueryRelation) -> bool:
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
        dataset = self.ref(schema)
        try:
            next(iter(self._client.list_tables(dataset, max_results=1)))
        except StopIteration:
            pass
        except NotFound:
            return False
        return True

    @staticmethod
    def ref(schema: BaseRelation) -> DatasetReference:
        return DatasetReference(schema.database, schema.schema)
