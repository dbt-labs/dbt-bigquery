from dbt.contracts.relation import RelationType

from dbt.adapters.bigquery.relation import BigQueryRelation


def test_renameable_relation():
    relation = BigQueryRelation.create(
        database="my_db",
        schema="my_schema",
        identifier="my_table",
        type=RelationType.Table,
    )
    assert relation.renameable_relations == frozenset(
        {
            RelationType.Table,
        }
    )
