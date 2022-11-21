import pytest
from dbt.tests.util import run_dbt, check_relations_equal, check_relations_equal_with_relations
from tests.functional.test_override_database.fixtures import (
    models,
    seeds,
    project_files
)
import os


class BaseOverrideDatabase:
    @pytest.fixture(scope="class")
    def model_path(self):
        return "models"

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "config-version": 2,
            "seed-paths": ["seeds"],
            "vars": {
                "alternate_db": os.getenv("BIGQUERY_TEST_ALT_DATABASE"),
            },
            "quoting": {
                "database": True,
            },
            "seeds": {
                "quote_columns": False,
            }
        }

    @pytest.fixture(scope="class")
    def clear_test_schema(self, project):
        yield
        relation = project.adapter.Relation.create(database=os.getenv("BIGQUERY_TEST_ALT_DATABASE"), schema=project.test_schema)
        project.adapter.drop_schema(relation)


class TestModelOverrideBigQuery(BaseOverrideDatabase):
    def run_database_override(self, project):
        run_dbt(["seed"])
        assert len(run_dbt(["run"])) == 4
        check_relations_equal_with_relations(project.adapter, [
            project.adapter.Relation.create(schema=project.test_schema, identifier="seed"),
            project.adapter.Relation.create(database=os.getenv("BIGQUERY_TEST_ALT_DATABASE"), schema=project.test_schema, identifier="view_2"),
            project.adapter.Relation.create(schema=project.test_schema, identifier="view_1"),
            project.adapter.Relation.create(schema=project.test_schema, identifier="view_3"),
            project.adapter.Relation.create(database=os.getenv("BIGQUERY_TEST_ALT_DATABASE"), schema=project.test_schema, identifier="view_4")
        ])

    def test_bigquery_database_override(self, clear_test_schema, project):
        self.run_database_override(project)


class BaseTestProjectModelOverrideBigQuery(BaseOverrideDatabase):
    def run_database_override(self, project):
        run_dbt(["seed"])
        assert len(run_dbt(["run"])) == 4
        self.assertExpectedRelations(project)

    def assertExpectedRelations(self, project):
        check_relations_equal_with_relations(project.adapter, [
            project.adapter.Relation.create(schema=project.test_schema, identifier="seed"),
            project.adapter.Relation.create(database=os.getenv("BIGQUERY_TEST_ALT_DATABASE"), schema=project.test_schema, identifier="view_2"),
            project.adapter.Relation.create(database=os.getenv("BIGQUERY_TEST_ALT_DATABASE"), schema=project.test_schema, identifier="view_1"),
            project.adapter.Relation.create(schema=project.test_schema, identifier="view_3"),
            project.adapter.Relation.create(database=os.getenv("BIGQUERY_TEST_ALT_DATABASE"), schema=project.test_schema, identifier="view_4")
        ])


class TestProjectModelOverrideBigQuery(BaseTestProjectModelOverrideBigQuery):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "config-version": 2,
            "vars": {
                "alternate_db": os.getenv("BIGQUERY_TEST_ALT_DATABASE"),
            },
            "models": {
                "database": os.getenv("BIGQUERY_TEST_ALT_DATABASE"),
                "test": {
                    "subfolder": {
                        "database": "{{ target.database }}"
                    }
                }
            },
            "seed-paths": ["seeds"],
            "vars": {
                "alternate_db": os.getenv("BIGQUERY_TEST_ALT_DATABASE"),
            },
            "quoting": {
                "database": True,
            },
            "seeds": {
                "quote_columns": False,
            }
        }

    def test_bigquery_database_override(self, clear_test_schema, project):
        self.run_database_override(project)


class TestProjectModelAliasOverrideBigQuery(BaseTestProjectModelOverrideBigQuery):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "config-version": 2,
            "vars": {
                "alternate_db": os.getenv("BIGQUERY_TEST_ALT_DATABASE"),
            },
            "models": {
                "project": os.getenv("BIGQUERY_TEST_ALT_DATABASE"),
                "test": {
                    "subfolder": {
                        "project": "{{ target.database }}"
                    }
                }
            },
            "seed-paths": ["seeds"],
            "vars": {
                "alternate_db": os.getenv("BIGQUERY_TEST_ALT_DATABASE"),
            },
            "quoting": {
                "database": True,
            },
            "seeds": {
                "quote_columns": False,
            }
        }

    def test_bigquery_project_override(self, clear_test_schema, project):
        self.run_database_override(project)


class TestProjectSeedOverrideBigQuery(BaseOverrideDatabase):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "config-version": 2,
            "seed-paths": ["seeds"],
            "vars": {
                "alternate_db": os.getenv("BIGQUERY_TEST_ALT_DATABASE"),
            },
            "seeds": {
                "database": os.getenv("BIGQUERY_TEST_ALT_DATABASE")
            }
        }

    def run_database_override(self, project):
        run_dbt(["seed"])
        assert len(run_dbt(["run"])) == 4
        check_relations_equal_with_relations(project.adapter, [
            project.adapter.Relation.create(database=os.getenv("BIGQUERY_TEST_ALT_DATABASE"), schema=project.test_schema, identifier="seed"),
            project.adapter.Relation.create(database=os.getenv("BIGQUERY_TEST_ALT_DATABASE"), schema=project.test_schema, identifier="view_2"),
            project.adapter.Relation.create(schema=project.test_schema, identifier="view_1"),
            project.adapter.Relation.create(schema=project.test_schema, identifier="view_3"),
            project.adapter.Relation.create(database=os.getenv("BIGQUERY_TEST_ALT_DATABASE"), schema=project.test_schema, identifier="view_4")
        ])

    def test_bigquery_database_override(self, clear_test_schema, project):
        self.run_database_override(project)
