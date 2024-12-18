from dbt.adapters.bigquery.column import BigQueryColumn
from dbt.adapters.bigquery.connections import BigQueryConnectionManager
from dbt.adapters.bigquery.credentials import BigQueryCredentials
from dbt.adapters.bigquery.impl import BigQueryAdapter, GrantTarget, PartitionConfig
from dbt.adapters.bigquery.relation import BigQueryRelation

from dbt.adapters.base import AdapterPlugin
from dbt.include import bigquery

Plugin = AdapterPlugin(
    adapter=BigQueryAdapter, credentials=BigQueryCredentials, include_path=bigquery.PACKAGE_PATH
)
