import pytest
from dbt.tests.util import (
    run_dbt,
    run_sql_with_adapter
)
import datetime
import yaml

_UPLOAD_FILE_SQL = """
{% macro upload_file(local_file_path, database, table_schema, table_name) %}
  {% do adapter.upload_file(local_file_path, database, table_schema, table_name, kwargs=kwargs) %}
{% endmacro %}
""".lstrip()


class TestUploadFile:
    @pytest.fixture(scope="class")
    def schema(self):
        return "upload_file"

    @pytest.fixture(scope="class")
    def macros(self):
        return {
            "upload_file.sql": _UPLOAD_FILE_SQL,
        }

    @staticmethod
    def perform_uploaded_table_checks(table_schema, table_name, project):
        # Test the data types of the created table
        data_type_query = f"""
            select
                column_name,
                data_type
            from `{table_schema}.INFORMATION_SCHEMA.COLUMNS`
            where
                table_name = '{table_name}'
            order by column_name
        """
        run_sql_results = run_sql_with_adapter(project.adapter, data_type_query)

        # The table should consist of columns of types: STRING, STRING, STRING, INT64, STRING, TIMESTAMP
        assert [row[1] for row in run_sql_results] == ['STRING', 'STRING', 'STRING', 'INT64', 'STRING', 'TIMESTAMP']

        # Test the values of the created table
        value_query = f"""
            select
                count(*) row_count,
                count(distinct id) as num_distinct_ids,
                max(updated_at) as max_updated_at
            from `{table_schema}.{table_name}`
        """
        value_results = run_sql_with_adapter(project.adapter, value_query)

        # There should be 100 rows in this table
        assert value_results[0][0] == 100
        # There should be 100 distinct id values in this table
        assert value_results[0][1] == 100
        # Maximum updated_at value should be 2016-09-19 14:45:51
        assert value_results[0][2] == datetime.datetime(2016, 9, 19, 14, 45, 51, tzinfo=datetime.timezone.utc)

    def test_bigquery_upload_file_csv(self, project):
        # Create a table from an uploaded CSV file
        upload_args = yaml.safe_dump({
            'local_file_path': f"{project.test_data_dir}/csv/source.csv",
            'database': project.database,
            'table_schema': project.test_schema,
            'table_name': 'TestUploadFileCSV',
            'skip_leading_rows': 1,
            'autodetect': True,
            'write_disposition': 'WRITE_TRUNCATE'
        })
        upload_result = run_dbt(['run-operation', 'upload_file', '--args', upload_args])
        assert upload_result.success

        # Check if the uploaded table contains expected values and schema
        self.perform_uploaded_table_checks(project.test_schema, 'TestUploadFileCSV', project)

    def test_bigquery_upload_file_ndjson(self, project):
        # Create a table from an uploaded NDJSON file
        upload_args = yaml.safe_dump({
            'local_file_path': f"{project.test_data_dir}/ndjson/source.ndjson",
            'database': project.database,
            'table_schema': project.test_schema,
            'table_name': 'TestUploadFileNDJSON',
            'autodetect': True,
            'source_format': 'NEWLINE_DELIMITED_JSON',
            'write_disposition': 'WRITE_TRUNCATE'
        })
        upload_result = run_dbt(['run-operation', 'upload_file', '--args', upload_args])
        assert upload_result.success

        # Check if the uploaded table contains expected values and schema
        self.perform_uploaded_table_checks(project.test_schema, 'TestUploadFileNDJSON', project)

    def test_bigquery_upload_file_parquet(self, project):
        # Create a table from an uploaded parquet file
        upload_args = yaml.safe_dump({
            'local_file_path': f"{project.test_data_dir}/parquet/source.parquet",
            'database': project.database,
            'table_schema': project.test_schema,
            'table_name': 'TestUploadFileParquet',
            'source_format': 'PARQUET',
            'write_disposition': 'WRITE_TRUNCATE'
        })
        upload_result = run_dbt(['run-operation', 'upload_file', '--args', upload_args])
        assert upload_result.success

        # Check if the uploaded table contains expected values and schema
        self.perform_uploaded_table_checks(project.test_schema, 'TestUploadFileParquet', project)
