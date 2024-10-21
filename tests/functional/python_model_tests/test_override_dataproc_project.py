import pytest

from dbt.tests.util import run_dbt
from tests.unit.test_bigquery_adapter import BaseTestBigQueryAdapter
from tests.functional.python_model_tests.files import SINGLE_RECORD  # noqa: F401
from unittest.mock import patch


# Test application of dataproc_batch configuration to a
# google.cloud.dataproc_v1.Batch object.
# This reuses the machinery from BaseTestBigQueryAdapter to get hold of the
# parsed credentials
class TestOverrideDataprocProject(BaseTestBigQueryAdapter):
    @pytest.fixture(scope="class")
    def model_path(self):
        return "models"

    def test_update_dataproc_cluster(self):
        # update the raw profile to set dataproc_project config
        self.raw_profile["outputs"]["dataproc-serverless-configured"]["dataproc_batch"][
            "dataproc_project"
        ] = "test"
        adapter = self.get_adapter("dataproc-serverless-configured")
        run_dbt(["models"])

        # raw_profile = self.raw_profile["outputs"]["dataproc-serverless-configured"][
        #    "dataproc_batch"
        # ]


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
