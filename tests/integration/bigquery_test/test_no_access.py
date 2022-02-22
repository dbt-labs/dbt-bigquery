from tests.integration.base import DBTIntegrationTest, use_profile
import dbt.exceptions


class TestNoAccess(DBTIntegrationTest):

    @property
    def schema(self):
        return 'no_access_models'

    @property
    def models(self):
        return "no-access-models"
    
    @use_profile('bigquery')
    def test_bigquery_no_access(self):
        """tests error exceptions against project user doesn't have access to."""
        results = self.run_dbt(['run','--exclude','model_1'])
        self.assertEqual(len(results), 1)
        results = self.run_dbt(['run','--select','model_2'])
        self.assertEqual(len(results), 1)
        try:
            results = self.run_dbt(['run','--select','model_1'])
        except dbt.exceptions.DatabaseException:
            # have to test against DatabaseException as run_dbt catches warehouse thorwn error and genreralizes it to dbt version.
            is_false = True
            self.assertTrue(is_false)
            self.assertRaises(dbt.exceptions.DatabaseException)
            