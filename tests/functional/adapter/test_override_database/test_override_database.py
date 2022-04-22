import pytest
from dbt.tests.util import run_dbt
from tests.functional.adapter.test_override_database.fixtures import (
    models,
    seeds,
    project_files
)
import os


class BaseOverrideDatabase:
    setup_alternate_db = True
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


class TestModelOverride(BaseOverrideDatabase):
    def run_database_override(self):
        func = lambda x: x

        run_dbt(['seed'])

        assert len(run_dbt(['run'])) == 4
        # self.assertManyRelationsEqual([
        #     (func('seed'), self.unique_schema(), self.default_database),
        #     (func('view_2'), self.unique_schema(), self.alternative_database),
        #     (func('view_1'), self.unique_schema(), self.default_database),
        #     (func('view_3'), self.unique_schema(), self.default_database),
        #     (func('view_4'), self.unique_schema(), self.alternative_database),
        # ])

    def test_bigquery_database_override(self, project):
        self.run_database_override()


class BaseTestProjectModelOverride(BaseOverrideDatabase):
    # this is janky, but I really want to access self.default_database in
    # project_config
    def default_database(self):
        target = self._profile_config['test']['target']
        profile = self._profile_config['test']['outputs'][target]
        for key in ['database', 'project', 'dbname']:
            if key in profile:
                database = profile[key]
                return database
        assert False, 'No profile database found!'

    def run_database_override(self):
        run_dbt(['seed'])
        assert len(run_dbt(['run'])) == 4
        # self.assertExpectedRelations()

    # def assertExpectedRelations(self):
    #     func = lambda x: x

    #     self.assertManyRelationsEqual([
    #         (func('seed'), self.unique_schema(), self.default_database),
    #         (func('view_2'), self.unique_schema(), self.alternative_database),
    #         (func('view_1'), self.unique_schema(), self.alternative_database),
    #         (func('view_3'), self.unique_schema(), self.default_database),
    #         (func('view_4'), self.unique_schema(), self.alternative_database),
    #     ])


class TestProjectModelOverride(BaseTestProjectModelOverride):
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
        self.run_database_override()


class TestProjectModelAliasOverride(BaseTestProjectModelOverride):
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
        self.run_database_override()


class TestProjectSeedOverride(BaseOverrideDatabase):
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
    def run_database_override(self):
        func = lambda x: x
        run_dbt(['seed'])
        assert len(run_dbt(['run'])) == 4
        # self.assertManyRelationsEqual([
        #     (func('seed'), self.unique_schema(), self.alternative_database),
        #     (func('view_2'), self.unique_schema(), self.alternative_database),
        #     (func('view_1'), self.unique_schema(), self.default_database),
        #     (func('view_3'), self.unique_schema(), self.default_database),
        #     (func('view_4'), self.unique_schema(), self.alternative_database),
        # ])

    def test_bigquery_database_override(self, project):
        self.run_database_override()
