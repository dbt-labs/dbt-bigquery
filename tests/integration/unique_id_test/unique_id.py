from tests.integration.base import DBTIntegrationTest, use_profile
import hashlib


class TestUniqueKey(DBTIntegrationTest):

    def setUp(self):
        super().setUp()

    @property
    def models(self):
        return 'models'


    @property
    def schema(self):
        return 'unique_id'

    @property
    def project_config(self):
        return {
            'config-version': 2,
            'seed-paths': ['seeds'],
        }
        
    @use_profile('bigquery')
    def test_bigquery_unique_key(self):
        self.run_dbt(['seed'])
        self.run_dbt(['run'])
        self.assertTablesEqual("result", "expected")
        