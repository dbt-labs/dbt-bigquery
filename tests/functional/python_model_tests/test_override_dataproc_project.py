import os
from unittest.mock import patch

from dbt.tests.util import run_dbt, check_relations_equal_with_relations
from tests.unit.test_bigquery_adapter import BaseTestBigQueryAdapter

from tests.functional.python_model_tests.files import SINGLE_RECORD  # noqa: F401

ALT_DATABASE = os.getenv("BIGQUERY_TEST_ALT_DATABASE")

"""
dataset: dbt_mwinkler_core_dev
keyfile: /Users/matt-winkler/Downloads/sales-demo-project-314714-2e886a5c2612.json
method: service-account
project: sales-demo-project-314714
threads: 4
type: bigquery
gcs_bucket: matt-w-python-demo
dataproc_cluster_name: matt-w-python-demo
dataproc_region: us-west1
dataproc_project: test-some-other-project
"""


# Test application of dataproc_batch configuration to a
# google.cloud.dataproc_v1.Batch object.
# This reuses the machinery from BaseTestBigQueryAdapter to get hold of the
# parsed credentials
class TestOverrideDataprocProject(BaseTestBigQueryAdapter):
    @patch(
        "dbt.adapters.bigquery.connections.get_bigquery_defaults",
        return_value=("credentials", "project_id"),
    )
    def test_update_dataproc_cluster(self):
        adapter = self.get_adapter("dataproc-serverless-configured")
        raw_profile = self.raw_profile["outputs"]["dataproc-serverless-configured"][
            "dataproc_batch"
        ]
        print("showing raw_profile")
        print(raw_profile)


# class BaseOverrideDatabase:
#     @pytest.fixture(scope="class")
#     def model_path(self):
#         return "models"

#     @pytest.fixture(scope="class")
#     def project_config_update(self):
#         return {
#             "config-version": 2,
#             "seed-paths": ["seeds"],
#             "vars": {
#                 "alternate_db": ALT_DATABASE,
#             },
#             "quoting": {
#                 "database": True,
#             },
#             "seeds": {
#                 "quote_columns": False,
#             },
#         }

#     @pytest.fixture(scope="function")
#     def clean_up(self, project):
#         yield
#         relation = project.adapter.Relation.create(
#             database=ALT_DATABASE, schema=project.test_schema
#         )
#         project.adapter.drop_schema(relation)


# class TestModelOverrideBigQuery(BaseOverrideDatabase):
#     def run_database_override(self, project):
#         run_dbt(["seed"])
#         assert len(run_dbt(["run"])) == 4
#         check_relations_equal_with_relations(
#             project.adapter,
#             [
#                 project.adapter.Relation.create(schema=project.test_schema, identifier="seed"),
#                 project.adapter.Relation.create(
#                     database=ALT_DATABASE, schema=project.test_schema, identifier="view_2"
#                 ),
#                 project.adapter.Relation.create(schema=project.test_schema, identifier="view_1"),
#                 project.adapter.Relation.create(schema=project.test_schema, identifier="view_3"),
#                 project.adapter.Relation.create(
#                     database=ALT_DATABASE, schema=project.test_schema, identifier="view_4"
#                 ),
#             ],
#         )

#     def test_bigquery_database_override(self, project, clean_up):
#         self.run_database_override(project)


# class BaseTestProjectModelOverrideBigQuery(BaseOverrideDatabase):
#     def run_database_override(self, project):
#         run_dbt(["seed"])
#         assert len(run_dbt(["run"])) == 4
#         self.assertExpectedRelations(project)

#     def assertExpectedRelations(self, project):
#         check_relations_equal_with_relations(
#             project.adapter,
#             [
#                 project.adapter.Relation.create(schema=project.test_schema, identifier="seed"),
#                 project.adapter.Relation.create(
#                     database=ALT_DATABASE, schema=project.test_schema, identifier="view_2"
#                 ),
#                 project.adapter.Relation.create(
#                     database=ALT_DATABASE, schema=project.test_schema, identifier="view_1"
#                 ),
#                 project.adapter.Relation.create(schema=project.test_schema, identifier="view_3"),
#                 project.adapter.Relation.create(
#                     database=ALT_DATABASE, schema=project.test_schema, identifier="view_4"
#                 ),
#             ],
#         )


# class TestProjectModelOverrideBigQuery(BaseTestProjectModelOverrideBigQuery):
#     @pytest.fixture(scope="class")
#     def project_config_update(self):
#         return {
#             "config-version": 2,
#             "models": {
#                 "database": ALT_DATABASE,
#                 "test": {"subfolder": {"database": "{{ target.database }}"}},
#             },
#             "seed-paths": ["seeds"],
#             "vars": {
#                 "alternate_db": ALT_DATABASE,
#             },
#             "quoting": {
#                 "database": True,
#             },
#             "seeds": {
#                 "quote_columns": False,
#             },
#         }

#     def test_bigquery_database_override(self, project, clean_up):
#         self.run_database_override(project)


# class TestProjectModelAliasOverrideBigQuery(BaseTestProjectModelOverrideBigQuery):
#     @pytest.fixture(scope="class")
#     def project_config_update(self):
#         return {
#             "config-version": 2,
#             "models": {
#                 "project": ALT_DATABASE,
#                 "test": {"subfolder": {"project": "{{ target.database }}"}},
#             },
#             "seed-paths": ["seeds"],
#             "vars": {
#                 "alternate_db": ALT_DATABASE,
#             },
#             "quoting": {
#                 "database": True,
#             },
#             "seeds": {
#                 "quote_columns": False,
#             },
#         }

#     def test_bigquery_project_override(self, project, clean_up):
#         self.run_database_override(project)


# class TestProjectSeedOverrideBigQuery(BaseOverrideDatabase):
#     @pytest.fixture(scope="class")
#     def project_config_update(self):
#         return {
#             "config-version": 2,
#             "seed-paths": ["seeds"],
#             "vars": {
#                 "alternate_db": ALT_DATABASE,
#             },
#             "seeds": {"database": ALT_DATABASE},
#         }

#     def run_database_override(self, project):
#         run_dbt(["seed"])
#         assert len(run_dbt(["run"])) == 4
#         check_relations_equal_with_relations(
#             project.adapter,
#             [
#                 project.adapter.Relation.create(
#                     database=ALT_DATABASE, schema=project.test_schema, identifier="seed"
#                 ),
#                 project.adapter.Relation.create(
#                     database=ALT_DATABASE, schema=project.test_schema, identifier="view_2"
#                 ),
#                 project.adapter.Relation.create(schema=project.test_schema, identifier="view_1"),
#                 project.adapter.Relation.create(schema=project.test_schema, identifier="view_3"),
#                 project.adapter.Relation.create(
#                     database=ALT_DATABASE, schema=project.test_schema, identifier="view_4"
#                 ),
#             ],
#         )

#     def test_bigquery_database_override(self, project, clean_up):
#         self.run_database_override(project)
