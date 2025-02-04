import time

import pytest

from dbt.tests.util import run_dbt


def select_1(dataset: str, materialized: str):
    config = f"""config(
                materialized='{materialized}',
                grant_access_to=[
                  {{'project': 'dbt-test-env', 'dataset': '{dataset}'}},
                ]
            )"""
    return (
        "{{"
        + config
        + "}}"
        + """
           SELECT 1 as one"""
    )


def select_1_materialized_view(dataset: str):
    config = f"""config(
                materialized='materialized_view',
                grant_access_to=[
                  {{'project': 'dbt-test-env', 'dataset': '{dataset}'}},
                ]
            )"""
    return (
        "{{"
        + config
        + "}}"
        + """
           SELECT one, COUNT(1) AS count_one
           FROM {{ ref('select_1_table') }}
           GROUP BY one"""
    )


BAD_CONFIG_TABLE_NAME = "bad_view"
BAD_CONFIG_TABLE = """
{{ config(
    materialized='view',
    grant_access_to=[
      {'project': 'dbt-test-env', 'dataset': 'NonExistentDataset'},
    ]
) }}

SELECT 1 as one
"""

BAD_CONFIG_CHILD_TABLE = "SELECT 1 as one FROM {{ref('" + BAD_CONFIG_TABLE_NAME + "')}}"


def get_schema_name(base_schema_name: str) -> str:
    return f"{base_schema_name}_grant_access"


class TestAccessGrantSucceeds:
    @pytest.fixture(scope="class")
    def setup_grant_schema(
        self,
        project,
        unique_schema,
    ):
        with project.adapter.connection_named("__test_grants"):
            relation = project.adapter.Relation.create(
                database=project.database,
                schema=get_schema_name(unique_schema),
                identifier="grant_access",
            )
            project.adapter.create_schema(relation)
            yield relation

    @pytest.fixture(scope="class")
    def teardown_grant_schema(
        self,
        project,
        unique_schema,
    ):
        yield
        with project.adapter.connection_named("__test_grants"):
            relation = project.adapter.Relation.create(
                database=project.database, schema=get_schema_name(unique_schema)
            )
            project.adapter.drop_schema(relation)

    @pytest.fixture(scope="class")
    def models(self, unique_schema):
        dataset = get_schema_name(unique_schema)
        return {
            "select_1.sql": select_1(dataset=dataset, materialized="view"),
            "select_1_table.sql": select_1(dataset=dataset, materialized="table"),
            "select_1_materialized_view.sql": select_1_materialized_view(dataset=dataset),
        }

    def test_grant_access_succeeds(self, project, setup_grant_schema, teardown_grant_schema, unique_schema):
        # Need to run twice to validate idempotency
        results = run_dbt(["run"])
        assert len(results) == 3
        time.sleep(10)
        # Materialized view excluded since it would produce an error since source table is replaced
        results = run_dbt(["run", "--exclude", "select_1_materialized_view"])
        assert len(results) == 2

        with project.adapter.connection_named("__test_grants"):
            client = project.adapter.connections.get_thread_connection().handle
            dataset_name = get_schema_name(unique_schema)
            dataset_id = "{}.{}".format("dbt-test-env", dataset_name)
            bq_dataset = client.get_dataset(dataset_id)

            authorized_view_names = []
            for access_entry in bq_dataset.access_entries:
                if access_entry.entity_type != "view":
                    continue

                authorized_view_names.append(access_entry.entity_id["tableId"])

            assert set(authorized_view_names) == set(["select_1", "select_1_materialized_view"])


class TestAccessGrantFails:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "bad_config_table_child.sql": BAD_CONFIG_CHILD_TABLE,
            f"{BAD_CONFIG_TABLE_NAME}.sql": BAD_CONFIG_TABLE,
        }

    def test_grant_access_fails_without_running_child_table(self, project):
        # Need to run twice to validate idempotency
        results = run_dbt(["run"], expect_pass=False)
        assert results[0].status == "error"
        assert results[1].status == "skipped"
        assert results[0].message.startswith("404 GET https://bigquery.googleapis.com/")
