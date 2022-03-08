from tests.integration.base import DBTIntegrationTest, use_profile
import datetime
import yaml


class TestUploadFileCSV(DBTIntegrationTest):
    @property
    def schema(self):
        return "upload_file"

    @property
    def models(self):
        return "models"
        
    def perform_uploaded_table_checks(self, table_schema, table_name):
        # Test the data types of the created table
        query = f"""
            select
                column_name,
                data_type
            from `{table_schema}.INFORMATION_SCHEMA.COLUMNS`
            where
                table_name = '{table_name}'
            order by column_name
        """
        results = self.run_sql(query, fetch='all')
        self.assertTrue([row[1] for row in results] == ['STRING', 'STRING', 'STRING', 'INT64', 'STRING', 'TIMESTAMP'], 'The table should consist of columns of types: STRING, STRING, STRING, INT64, STRING, TIMESTAMP')
        
        # Test the values of the created table
        query = f"""
            select
                count(*) row_count,
                count(distinct id) as num_distinct_ids,
                max(updated_at) as max_updated_at
            from `{table_schema}.{table_name}`
        """
        results = self.run_sql(query, fetch='all')
        self.assertTrue(results[0][0] == 100, 'There should be 100 rows in this table')
        self.assertTrue(results[0][1] == 100, 'There should be 100 distinct id values in this table')
        self.assertTrue(results[0][2] == datetime.datetime(2016, 9, 19, 14, 45, 51, tzinfo=datetime.timezone.utc), 'Maximum updated_at value should be 2016-09-19 14:45:51')

    @use_profile('bigquery')
    def test_bigquery_upload_file_csv(self):
        # Create a table from an uploaded CSV file
        upload_args = yaml.safe_dump({
            'local_file_path': './csv/source.csv',
            'database': self.default_database,
            'table_schema': self.unique_schema(),
            'table_name': 'TestUploadFileCSV',
            'skip_leading_rows': 1,
            'autodetect': True,
            'write_disposition': 'WRITE_TRUNCATE'
        })
        result = self.run_dbt(['run-operation', 'upload_file', '--args', upload_args])
        self.assertTrue(result.success)

        # Check uploaded table contains expected values and schema
        self.perform_uploaded_table_checks(self.unique_schema(), 'TestUploadFileCSV')

    @use_profile('bigquery')
    def test_bigquery_upload_file_ndjson(self):
        # Create a table from an uploaded NDJSON file
        upload_args = yaml.safe_dump({
            'local_file_path': './ndjson/source.ndjson',
            'database': self.default_database,
            'table_schema': self.unique_schema(),
            'table_name': 'TestUploadFileNDJSON',
            'autodetect': True,
            'source_format': 'NEWLINE_DELIMITED_JSON',
            'write_disposition': 'WRITE_TRUNCATE'
        })
        result = self.run_dbt(['run-operation', 'upload_file', '--args', upload_args])
        self.assertTrue(result.success)

        # Check uploaded table contains expected values and schema
        self.perform_uploaded_table_checks(self.unique_schema(), 'TestUploadFileNDJSON')

    @use_profile('bigquery')
    def test_bigquery_upload_file_parquet(self):
        # Create a table from an uploaded parquet file
        upload_args = yaml.safe_dump({
            'local_file_path': './parquet/source.parquet',
            'database': self.default_database,
            'table_schema': self.unique_schema(),
            'table_name': 'TestUploadFileParquet',
            'source_format': 'PARQUET',
            'write_disposition': 'WRITE_TRUNCATE'
        })
        result = self.run_dbt(['run-operation', 'upload_file', '--args', upload_args])
        self.assertTrue(result.success)

        # Check uploaded table contains expected values and schema
        self.perform_uploaded_table_checks(self.unique_schema(), 'TestUploadFileParquet')
