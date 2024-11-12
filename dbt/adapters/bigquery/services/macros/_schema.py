from dbt.adapters.bigquery.relation import BigQueryRelation
from dbt.adapters.bigquery.services.macros._execute import macro


# BigQuery added SQL support for 'create schema' + 'drop schema' in March 2021
# Unfortunately, 'drop schema' runs into permissions issues during tests
# Most of the value here comes from user overrides of 'create_schema'


# TODO: the code below is copy-pasted from SQLAdapter.create_schema. Is there a better way?
def create_schema(relation: BigQueryRelation) -> None:
    schema = relation.without_identifier()
    return macro("create_schema", kwargs={"relation": schema})
