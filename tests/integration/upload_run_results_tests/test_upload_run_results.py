from tests.integration.base import DBTIntegrationTest, use_profile
import datetime
import yaml


class TestUploadRunResults(DBTIntegrationTest):
    @property
    def schema(self):
        return "upload_run_results"

    @property
    def models(self):
        return "models"
        
    def perform_uploaded_run_results_checks(self, table_schema, table_name):
        # Test the values of the created table
        query = f"""
            select
                args.event_buffer_size,
                elapsed_time,
                metadata.generated_at,
                results[OFFSET(0)].unique_id
            from `{table_schema}.{table_name}`
        """
        results = self.run_sql(query, fetch='all')
        
        self.assertTrue(results[0][0] == 100000, 'Testing nested INT data type')
        self.assertTrue(float(results[0][1]) == 3.7216997146606445, 'Testing non-nested INT data type')
        self.assertTrue(results[0][2] == datetime.datetime(2022, 4, 3, 14, 12, 1, 509442, tzinfo=datetime.timezone.utc), 'Testing TIMESTAMP data type')
        self.assertTrue(results[0][3] == "model.dbt_proj.my_first_dbt_model", 'Testing STRING data type within REPEATED field')

    @use_profile('bigquery')
    def test_bigquery_upload_run_resultsv2(self):
        # Create a table from an uploaded run_results.json file
        upload_args = yaml.safe_dump({
            'local_file_path': './run_results.json',
            'database': self.default_database,
            'table_schema': self.unique_schema(),
            'table_name': 'TestUploadRunResults',
            'write_disposition': 'WRITE_TRUNCATE'
        })
        result = self.run_dbt(['run-operation', 'upload_run_results', '--args', upload_args])
        self.assertTrue(result.success)

        # Check uploaded run_results table contains expected values
        self.perform_uploaded_run_results_checks(self.unique_schema(), 'TestUploadRunResults')
