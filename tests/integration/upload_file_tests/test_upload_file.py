from tests.integration.base import DBTIntegrationTest, use_profile
import yaml


class TestUploadFileCSV(DBTIntegrationTest):
    @property
    def schema(self):
        return "upload_file"

    @property
    def models(self):
        return "models"

    @use_profile('bigquery')
    def test_bigquery_upload_file_csv(self):
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


class TestUploadFileNDJSON(DBTIntegrationTest):
    @property
    def schema(self):
        return "upload_file"

    @property
    def models(self):
        return "models"

    @use_profile('bigquery')
    def test_bigquery_upload_file_ndjson(self):
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


class TestUploadFileParquet(DBTIntegrationTest):
    @property
    def schema(self):
        return "upload_file"

    @property
    def models(self):
        return "models"

    @use_profile('bigquery')
    def test_bigquery_upload_file_parquet(self):
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