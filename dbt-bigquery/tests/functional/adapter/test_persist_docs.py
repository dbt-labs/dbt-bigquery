import json
import pytest

from dbt.tests.util import run_dbt

from dbt.tests.adapter.persist_docs.test_persist_docs import (
    BasePersistDocsBase,
    BasePersistDocs,
)

_MODELS__TABLE_MODEL_NESTED = """
{{ config(materialized='table') }}
SELECT
    STRUCT(
        STRUCT(
            1 AS level_3_a,
            2 AS level_3_b
        ) AS level_2
    ) AS level_1
"""

_MODELS__VIEW_MODEL_NESTED = """
{{ config(materialized='view') }}
SELECT
    STRUCT(
        STRUCT(
            1 AS level_3_a,
            2 AS level_3_b
        ) AS level_2
    ) AS level_1
"""

_PROPERTIES__MODEL_COMMENTS = """
version: 2

models:
  - name: table_model_nested
    columns:
      - name: level_1
        description: level_1 column description
      - name: level_1.level_2
        description: level_2 column description
      - name: level_1.level_2.level_3_a
        description: level_3 column description
  - name: view_model_nested
    columns:
      - name: level_1
        description: level_1 column description
      - name: level_1.level_2
        description: level_2 column description
      - name: level_1.level_2.level_3_a
        description: level_3 column description
"""


class TestBasePersistDocs(BasePersistDocs):
    def _assert_has_view_comments(
        self, view_node, has_node_comments=True, has_column_comments=True
    ):
        view_comment = view_node["metadata"]["comment"]
        if has_node_comments:
            assert view_comment.startswith("View model description")
            self._assert_common_comments(view_comment)
        else:
            assert not view_comment

        view_id_comment = view_node["columns"]["id"]["comment"]
        if has_column_comments:
            assert view_id_comment.startswith("id Column description")
            self._assert_common_comments(view_id_comment)
        else:
            assert not view_id_comment

        view_name_comment = view_node["columns"]["name"]["comment"]
        assert not view_name_comment


class TestPersistDocsSimple(BasePersistDocsBase):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "models": {
                "test": {
                    "+persist_docs": {
                        "relation": True,
                        "columns": True,
                    },
                }
            },
            "seeds": {
                "test": {
                    "+persist_docs": {
                        "relation": True,
                        "columns": True,
                    },
                }
            },
        }

    def test_persist_docs(self, project):
        run_dbt(["seed"])
        run_dbt()
        desc_map = {
            "seed": "Seed model description",
            "table_model": "Table model description",
            "view_model": "View model description",
        }
        for node_id in ["seed", "table_model", "view_model"]:
            with project.adapter.connection_named("_test"):
                client = project.adapter.connections.get_thread_connection().handle

                table_id = "{}.{}.{}".format(project.database, project.test_schema, node_id)
                bq_table = client.get_table(table_id)

                bq_schema = bq_table.schema

                assert bq_table.description.startswith(desc_map[node_id])
                assert bq_schema[0].description.startswith("id Column description ")
                if not node_id.startswith("view"):
                    assert bq_schema[1].description.startswith(
                        "Some stuff here and then a call to"
                    )


class TestPersistDocsColumnMissing(BasePersistDocsBase):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "models": {
                "test": {
                    "+persist_docs": {
                        "columns": True,
                    },
                }
            }
        }

    def test_missing_column(self, project):
        run_dbt()


class TestPersistDocsNested(BasePersistDocsBase):
    @pytest.fixture(scope="class")
    def properties(self):
        return {"schema.yml": _PROPERTIES__MODEL_COMMENTS}

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "table_model_nested.sql": _MODELS__TABLE_MODEL_NESTED,
            "view_model_nested.sql": _MODELS__VIEW_MODEL_NESTED,
        }

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "models": {
                "test": {
                    "+persist_docs": {
                        "relation": True,
                        "columns": True,
                    },
                }
            }
        }

    def test_persist_docs(self, project):
        """
        run dbt and use the bigquery client from the adapter to check if the
        colunmn descriptions are persisted on the test model table and view.

        Next, generate the catalog and check if the comments are also included.

        Note: dbt-bigquery does not allow comments on models with children nodes
        """
        run_dbt(["seed"])
        run_dbt()

        run_dbt(["docs", "generate"])
        with open("target/catalog.json") as fp:
            catalog_data = json.load(fp)
        assert "nodes" in catalog_data
        assert len(catalog_data["nodes"]) == 3  # seed, table, and view model

        for node_id in ["table_model_nested", "view_model_nested"]:
            # check the descriptions using the api
            with project.adapter.connection_named("_test"):
                client = project.adapter.connections.get_thread_connection().handle

                table_id = "{}.{}.{}".format(project.database, project.test_schema, node_id)
                bq_schema = client.get_table(table_id).schema

                level_1_field = bq_schema[0]
                assert level_1_field.description == "level_1 column description"

                level_2_field = level_1_field.fields[0]
                assert level_2_field.description == "level_2 column description"

                level_2_field = level_1_field.fields[0]
                level_3_field = level_2_field.fields[0]
                assert level_3_field.description == "level_3 column description"

            # check the descriptions in the catalog
            node = catalog_data["nodes"]["model.test.{}".format(node_id)]

            level_1_column = node["columns"]["level_1"]
            assert level_1_column["comment"] == "level_1 column description"

            level_2_column = node["columns"]["level_1.level_2"]
            assert level_2_column["comment"] == "level_2 column description"

            level_3_column = node["columns"]["level_1.level_2.level_3_a"]
            assert level_3_column["comment"] == "level_3 column description"
