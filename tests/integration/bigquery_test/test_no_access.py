from tests.integration.base import DBTIntegrationTest, use_profile


class TestNoAccess(DBTIntegrationTest):

    @property
    def schema(self):
        return 'no_access_models'

    @property
    def models(self):
        return "no-access-models"
    
    @use_profile('bigquery')
    def test_bigquery_no_access(self):
        results = self.run_dbt(['run','--exclude','model_1'])
        self.assertEqual(len(results), 1)
        results = self.run_dbt(['run','--select','model_2'])
        self.assertEqual(len(results), 1)
        
        