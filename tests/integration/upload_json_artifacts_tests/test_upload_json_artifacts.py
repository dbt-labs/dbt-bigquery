from tests.integration.base import DBTIntegrationTest, use_profile
import datetime
import yaml


class TestUploadJsonArtifacts(DBTIntegrationTest):
    @property
    def schema(self):
        return "upload_json_artifacts"

    @property
    def models(self):
        return "models"
        
    def perform_uploaded_json_artifacts_checks_one_row(self, table_schema):
        # Test that each artifact results in a table with one row
        for artifact in ["catalog", "manifest", "run_results", "sources"]:
            query = f"""
                select
                    row_count
                from `{table_schema}.__TABLES__`
                where
                    table_id = "{artifact}"
            """
            results = self.run_sql(query, fetch='all')
            
            self.assertTrue(results[0][0] == 1, f"Testing {artifact}.json upload resulted in a single row table")
        
    def perform_uploaded_json_artifacts_checks_catalog_nesting(self, table_schema):
        query = f"""
            select
                nodes.model__dbt_proj__src_my_first_source.columns.id.name
            from `{table_schema}.catalog`
        """
        results = self.run_sql(query, fetch='all')
        
        self.assertTrue(results[0][0] == "id", f"Testing catalog.json upload resulted in properly nested columns")
        
    def perform_uploaded_json_artifacts_checks_manifest_nesting(self, table_schema):
        query = f"""
            select
                nodes.model__dbt_proj__my_first_dbt_model.columns.id.name
            from `{table_schema}.manifest` c
        """
        results = self.run_sql(query, fetch='all')
        
        self.assertTrue(results[0][0] == "id", f"Testing manifest.json upload resulted in properly nested columns")
        
    def perform_uploaded_json_artifacts_checks_run_results_nesting(self, table_schema):
        query = f"""
            select
                t.completed_at
            from (
                select
                    r.timing
                from `{table_schema}.run_results` 
                cross join unnest(results) as r
                where
                    r.unique_id = "test.dbt_proj.not_null_my_second_dbt_model_id.151b76d778"
            )
            cross join unnest(timing) as t
            where
                name = "execute"
        """
        results = self.run_sql(query, fetch='all')
        
        self.assertTrue(results[0][0] == datetime.datetime(2022, 4, 24, 15, 16, 59, 672871, tzinfo=datetime.timezone.utc), f"Testing run_results.json upload resulted in properly nested columns")
        
    def perform_uploaded_json_artifacts_checks_sources_nesting(self, table_schema):
        query = f"""
            select
                t.completed_at
            from (
                select
                    r.timing
                from `{table_schema}.sources` 
                cross join unnest(results) as r
            )
            cross join unnest(timing) as t
            where
                name = "execute"
        """
        results = self.run_sql(query, fetch='all')
        
        self.assertTrue(results[0][0] == datetime.datetime(2022, 4, 24, 15, 17, 51, 754899 , tzinfo=datetime.timezone.utc), f"Testing sources.json upload resulted in properly nested columns")

    @use_profile('bigquery')
    def test_bigquery_upload_json_artifacts(self):
        # Create a table from an uploaded run_results.json file
        upload_args = yaml.safe_dump({
            'local_file_path': './artifacts',
            'database': self.default_database,
            'table_schema': self.unique_schema(),
            'write_disposition': 'WRITE_TRUNCATE'
        })
        result = self.run_dbt(['run-operation', 'upload_json_artifacts', '--args', upload_args])
        self.assertTrue(result.success)

        # Check uploaded artifacts contains expected values
        self.perform_uploaded_json_artifacts_checks_one_row(self.unique_schema())
        self.perform_uploaded_json_artifacts_checks_catalog_nesting(self.unique_schema())
        self.perform_uploaded_json_artifacts_checks_manifest_nesting(self.unique_schema())
        self.perform_uploaded_json_artifacts_checks_run_results_nesting(self.unique_schema())
        self.perform_uploaded_json_artifacts_checks_sources_nesting(self.unique_schema())
