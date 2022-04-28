import pytest
from dbt.tests.util import run_dbt, check_relations_equal, check_relations_equal_with_relations
from tests.functional.adapter.test_override_database.fixtures import (
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
            'config-version': 2,
            'seed-paths': ['seeds'],
            'vars': {
                'alternate_db': os.getenv('BIGQUERY_TEST_ALT_DATABASE'),
            },
            'quoting': {
                'database': True,
            },
            'seeds': {
                'quote_columns': False,
            }
        }


class TestModelOverrideBigQuery(BaseOverrideDatabase):
    def run_database_override(self, project):
        # func = lambda x: x
        run_dbt(['seed'])
        assert len(run_dbt(['run'])) == 4
        # breakpoint()
        check_relations_equal_with_relations(project.adapter, [
            project.adapter.Relation.create(database=os.getenv('BIGQUERY_TEST_DATABASE'), schema=project.test_schema, identifier='seed'),
            project.adapter.Relation.create(database=os.getenv('BIGQUERY_TEST_DATABASE'), schema=project.test_schema, identifier='view_1'),
            # project.adapter.Relation.create(database=os.getenv('BIGQUERY_TEST_ALT_DATABASE'), schema=project.test_schema, identifier='view_2'),
            # project.adapter.Relation.create(database=os.getenv('BIGQUERY_TEST_DATABASE'), schema=project.test_schema, identifier='view_3'),
            # project.adapter.Relation.create(database=os.getenv('BIGQUERY_TEST_ALT_DATABASE'), schema=project.test_schema, identifier='view_4')
            # (os.getenv('BIGQUERY_TEST_ALT_DATABASE'), project.test_schema, 'seed'),
            # (os.getenv('BIGQUERY_TEST_DATABASE'), project.test_schema, 'view_1'),
            # (os.getenv('BIGQUERY_TEST_ALT_DATABASE'), project.test_schema, 'view_2'),
            # (os.getenv('BIGQUERY_TEST_DATABASE'), project.test_schema, 'view_3'),
            # (os.getenv('BIGQUERY_TEST_ALT_DATABASE'), project.test_schema, 'view_4'),
        ])


    def test_bigquery_database_override(self, project):
        self.run_database_override(project)


class BaseTestProjectModelOverrideBigQuery(BaseOverrideDatabase):
    # this is janky, but I really want to access self.default_database in
    # project_config
    # def default_database(self):
    #     target = self._profile_config['test']['target']
    #     profile = self._profile_config['test']['outputs'][target]
    #     for key in ['database', 'project', 'dbname']:
    #         if key in profile:
    #             database = profile[key]
    #             return database
    #     assert False, 'No profile database found!'


    def run_database_override(self, project):
        run_dbt(['seed'])
        assert len(run_dbt(['run'])) == 4
        self.assertExpectedRelations(project)

    def assertExpectedRelations(self, project):
        func = lambda x: x
        check_relations_equal_with_relations(project.adapter, [
            (func('seed'), project.test_schema, os.getenv('BIGQUERY_TEST_DATABASE')),
            (func('view_1'), project.test_schema, os.getenv('BIGQUERY_TEST_ALT_DATABASE')),
            (func('view_2'), project.test_schema, os.getenv('BIGQUERY_TEST_ALT_DATABASE')),
            (func('view_3'), project.test_schema, os.getenv('BIGQUERY_TEST_DATABASE')),
            (func('view_4'), project.test_schema, os.getenv('BIGQUERY_TEST_ALT_DATABASE')),
        ])


class TestProjectModelOverrideBigQuery(BaseTestProjectModelOverrideBigQuery):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            'config-version': 2,
            'vars': {
                'alternate_db': os.getenv('BIGQUERY_TEST_ALT_DATABASE'),
            },
            'models': {
                'database': os.getenv('BIGQUERY_TEST_ALT_DATABASE'),
                'test': {
                    'subfolder': {
                        'database': os.getenv('BIGQUERY_TEST_DATABASE')
                    }
                }
            },
            'seed-paths': ['seeds'],
            'vars': {
                'alternate_db': os.getenv('BIGQUERY_TEST_ALT_DATABASE'),
            },
            'quoting': {
                'database': True,
            },
            'seeds': {
                'quote_columns': False,
            }
        }

    def test_bigquery_database_override(self, project):
        self.run_database_override(project)


class TestProjectModelAliasOverrideBigQuery(BaseTestProjectModelOverrideBigQuery):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            'config-version': 2,
            'vars': {
                'alternate_db': os.getenv('BIGQUERY_TEST_ALT_DATABASE'),
            },
            'models': {
                'project': os.getenv('BIGQUERY_TEST_ALT_DATABASE'),
                'test': {
                    'subfolder': {
                        'project': os.getenv('BIGQUERY_TEST_DATABASE'),
                    }
                }
            },
            'seed-paths': ['seeds'],
            'vars': {
                'alternate_db': os.getenv('BIGQUERY_TEST_ALT_DATABASE'),
            },
            'quoting': {
                'database': True,
            },
            'seeds': {
                'quote_columns': False,
            }
        }

    def test_bigquery_project_override(self, project):
        self.run_database_override(project)


class TestProjectSeedOverrideBigQuery(BaseOverrideDatabase):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            'config-version': 2,
            'seed-paths': ['seeds'],
            'vars': {
                'alternate_db': os.getenv('BIGQUERY_TEST_ALT_DATABASE'),
            },
            'seeds': {
                'database': os.getenv('BIGQUERY_TEST_ALT_DATABASE')
            }
        }
    def run_database_override(self, project):
        func = lambda x: x
        run_dbt(['seed'])
        assert len(run_dbt(['run'])) == 4
        check_relations_equal_with_relations(project.adapter, [
            (func('seed'), project.test_schema, os.getenv('BIGQUERY_TEST_ALT_DATABASE')),
            (func('view_1'), project.test_schema, os.getenv('BIGQUERY_TEST_DATABASE')),
            (func('view_2'), project.test_schema, os.getenv('BIGQUERY_TEST_ALT_DATABASE')),
            (func('view_3'), project.test_schema, os.getenv('BIGQUERY_TEST_DATABASE')),
            (func('view_4'), project.test_schema, os.getenv('BIGQUERY_TEST_ALT_DATABASE')),
        ])

    def test_bigquery_database_override(self, project):
        self.run_database_override(project)
