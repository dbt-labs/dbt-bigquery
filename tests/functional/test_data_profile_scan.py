import os
import pytest
import yaml
from unittest.mock import patch, MagicMock
from dbt.adapters.bigquery.relation import BigQueryRelation
from dbt.tests.util import (
    run_dbt,
    get_connection,
    relation_from_name,
    read_file,
    write_config_file,
)

SCAN_LOCATION = "us-central1"
SCAN_ID = "bigquery_data_profile_scan_test"
MODEL_NAME = "test_model"

ORIGINAL_LABELS = {
    "my_label_key": "my_label_value",
}

PROFILE_SCAN_LABELS = [
    "dataplex-dp-published-scan",
    "dataplex-dp-published-project",
    "dataplex-dp-published-location",
]

SQL_CONTENT = """
{{
    config(
        materialized="table"
    )
}}
    select 20 as id, cast('2020-01-01 01:00:00' as datetime) as date_hour union all
    select 40 as id, cast('2020-01-01 02:00:00' as datetime) as date_hour
"""

YAML_CONTENT = f"""version: 2
models:
  - name: {MODEL_NAME}
"""

YAML_CONTENT_WITH_PROFILE_SCAN_SETTING = f"""version: 2
models:
  - name: {MODEL_NAME}
    config:
      data_profile_scan:
        location: us-central1
        scan_id: {SCAN_ID}
        sampling_percent: 10
        row_filter: "TRUE"
        cron: "CRON_TZ=Asia/New_York 0 9 * * *"
"""

INCREMENTAL_MODEL_CONTENT = """
{{
    config(
        materialized="incremental",
    )
}}

{% if not is_incremental() %}

    select 10 as id, cast('2020-01-01 01:00:00' as datetime) as date_hour union all
    select 30 as id, cast('2020-01-01 02:00:00' as datetime) as date_hour

{% else %}

    select 20 as id, cast('2020-01-01 01:00:00' as datetime) as date_hour union all
    select 40 as id, cast('2020-01-01 02:00:00' as datetime) as date_hour

{% endif %}
"""


class TestDataProfileScanWithProjectProfileScanSetting:
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "models": {
                "+labels": ORIGINAL_LABELS,
                "+data_profile_scan": {
                    "location": SCAN_LOCATION,
                    "sampling_percent": 10,
                    "row_filter": "TRUE",
                },
            },
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {
            f"{MODEL_NAME}.sql": SQL_CONTENT,
            f"{MODEL_NAME}.yml": YAML_CONTENT,
        }

    def test_create_data_profile_scan(self, project):
        with patch("google.cloud.dataplex_v1.DataScanServiceClient") as MockDataScanClient:
            mock_data_scan_client = MockDataScanClient.return_value

            results = run_dbt()
            assert len(results) == 1

            mock_data_scan_client.create_data_scan.assert_called_once()
            mock_data_scan_client.run_data_scan.assert_called_once()

            relation: BigQueryRelation = relation_from_name(project.adapter, MODEL_NAME)
            with get_connection(project.adapter) as conn:
                table = conn.handle.get_table(
                    project.adapter.connections.get_bq_table(
                        relation.database, relation.schema, relation.table
                    )
                )
                labels_to_be_created = PROFILE_SCAN_LABELS + list(ORIGINAL_LABELS.keys())
                assert set(table.labels.keys()) == set(labels_to_be_created)


class TestDataProfileScanWithProjectProfileScanSettingAndCron:
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "models": {
                "+labels": ORIGINAL_LABELS,
                "+data_profile_scan": {
                    "location": SCAN_LOCATION,
                    "scan_id": SCAN_ID,
                    "sampling_percent": 10,
                    "row_filter": "TRUE",
                    "cron": "CRON_TZ=Asia/New_York 0 9 * * *",
                },
            },
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {
            f"{MODEL_NAME}.sql": SQL_CONTENT,
            f"{MODEL_NAME}.yml": YAML_CONTENT,
        }

    def test_create_data_profile_scan(self, project):
        with patch("google.cloud.dataplex_v1.DataScanServiceClient") as MockDataScanClient:
            mock_data_scan_client = MockDataScanClient.return_value

            results = run_dbt()
            assert len(results) == 1

            mock_data_scan_client.create_data_scan.assert_called_once()
            mock_data_scan_client.run_data_scan.assert_not_called()

            relation: BigQueryRelation = relation_from_name(project.adapter, MODEL_NAME)
            with get_connection(project.adapter) as conn:
                table = conn.handle.get_table(
                    project.adapter.connections.get_bq_table(
                        relation.database, relation.schema, relation.table
                    )
                )
                labels_to_be_created = PROFILE_SCAN_LABELS + list(ORIGINAL_LABELS.keys())
                assert set(table.labels.keys()) == set(labels_to_be_created)


class TestDataProfileScanWithModelProfileScanSetting:
    @pytest.fixture(scope="class")
    def models(self):
        sql_content = f"""
        {{{{
            config(
                materialized="table",
                labels={ORIGINAL_LABELS},
            )
        }}}}
            select 20 as id, cast('2020-01-01 01:00:00' as datetime) as date_hour union all
            select 40 as id, cast('2020-01-01 02:00:00' as datetime) as date_hour
        """

        return {
            f"{MODEL_NAME}.sql": sql_content,
            f"{MODEL_NAME}.yml": YAML_CONTENT_WITH_PROFILE_SCAN_SETTING,
        }

    def test_create_data_profile_scan(self, project):
        with patch("google.cloud.dataplex_v1.DataScanServiceClient") as MockDataScanClient:
            mock_data_scan_client = MockDataScanClient.return_value

            results = run_dbt()
            assert len(results) == 1

            mock_data_scan_client.create_data_scan.assert_called_once()
            mock_data_scan_client.run_data_scan.assert_not_called()

            relation: BigQueryRelation = relation_from_name(project.adapter, MODEL_NAME)
            with get_connection(project.adapter) as conn:
                table = conn.handle.get_table(
                    project.adapter.connections.get_bq_table(
                        relation.database, relation.schema, relation.table
                    )
                )
                labels_to_be_created = PROFILE_SCAN_LABELS + list(ORIGINAL_LABELS.keys())
                assert set(table.labels.keys()) == set(labels_to_be_created)


class TestDataProfileScanWithoutProfileScanSetting:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            f"{MODEL_NAME}.sql": SQL_CONTENT,
            f"{MODEL_NAME}.yml": YAML_CONTENT,
        }

    def test_create_data_profile_scan(self, project):
        with patch("google.cloud.dataplex_v1.DataScanServiceClient") as MockDataScanClient:
            mock_data_scan_client = MockDataScanClient.return_value

            results = run_dbt()
            assert len(results) == 1

            mock_data_scan_client.create_data_scan.assert_not_called()
            mock_data_scan_client.run_data_scan.assert_not_called()

            relation: BigQueryRelation = relation_from_name(project.adapter, MODEL_NAME)
            with get_connection(project.adapter) as conn:
                table = conn.handle.get_table(
                    project.adapter.connections.get_bq_table(
                        relation.database, relation.schema, relation.table
                    )
                )
                labels_to_be_created = []
                assert set(table.labels.keys()) == set(labels_to_be_created)


class TestDataProfileScanDisabledProfileScanSetting:
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "models": {
                "+data_profile_scan": {
                    "location": SCAN_LOCATION,
                    "scan_id": SCAN_ID,
                    "enabled": False,
                },
            },
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {
            f"{MODEL_NAME}.sql": SQL_CONTENT,
            f"{MODEL_NAME}.yml": YAML_CONTENT,
        }

    def test_create_data_profile_scan(self, project):
        with patch("google.cloud.dataplex_v1.DataScanServiceClient") as MockDataScanClient:
            mock_data_scan_client = MockDataScanClient.return_value

            results = run_dbt()
            assert len(results) == 1

            mock_data_scan_client.create_data_scan.assert_not_called()
            mock_data_scan_client.run_data_scan.assert_not_called()

            relation: BigQueryRelation = relation_from_name(project.adapter, MODEL_NAME)
            with get_connection(project.adapter) as conn:
                table = conn.handle.get_table(
                    project.adapter.connections.get_bq_table(
                        relation.database, relation.schema, relation.table
                    )
                )
                labels_to_be_created = []
                assert set(table.labels.keys()) == set(labels_to_be_created)


class TestDataProfileScanUpdatedMidway:
    project_name = "my-project"

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "models": {
                "+database": self.project_name,
                "+labels": ORIGINAL_LABELS,
                "+data_profile_scan": {
                    "location": SCAN_LOCATION,
                    "scan_id": SCAN_ID,
                    "sampling_percent": 10,
                    "row_filter": "TRUE",
                },
            },
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {
            f"{MODEL_NAME}.sql": SQL_CONTENT,
            f"{MODEL_NAME}.yml": YAML_CONTENT,
        }

    def test_create_data_profile_scan(self, project):
        with patch("google.cloud.dataplex_v1.DataScanServiceClient") as MockDataScanClient:
            mock_data_scan_client = MockDataScanClient.return_value

            results = run_dbt()
            assert len(results) == 1

            mock_data_scan_client.create_data_scan.assert_called_once()
            mock_data_scan_client.run_data_scan.assert_called_once()

            def list_data_scans_mock(parent):
                mock_scan = MagicMock()
                mock_scan.name = SCAN_ID
                return [mock_scan]

            mock_data_scan_client.list_data_scans.side_effect = list_data_scans_mock

            project_yml = os.path.join(project.project_root, "dbt_project.yml")
            config = yaml.safe_load(read_file(project_yml))
            config["models"]["+data_profile_scan"]["sampling_percent"] = None
            write_config_file(config, project_yml)

            results = run_dbt()
            assert len(results) == 1
            mock_data_scan_client.update_data_scan.assert_called_once()

            relation: BigQueryRelation = relation_from_name(project.adapter, MODEL_NAME)
            with get_connection(project.adapter) as conn:
                table = conn.handle.get_table(
                    project.adapter.connections.get_bq_table(
                        relation.database, relation.schema, relation.table
                    )
                )
                labels_to_be_created = PROFILE_SCAN_LABELS + list(ORIGINAL_LABELS.keys())
                assert set(table.labels.keys()) == set(labels_to_be_created)


class TestDataProfileScanDisabledMidway:
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "models": {
                "+labels": ORIGINAL_LABELS,
                "+data_profile_scan": {
                    "location": SCAN_LOCATION,
                    "scan_id": SCAN_ID,
                    "sampling_percent": 10,
                    "row_filter": "TRUE",
                },
            },
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {
            f"{MODEL_NAME}.sql": SQL_CONTENT,
            f"{MODEL_NAME}.yml": YAML_CONTENT,
        }

    def test_create_data_profile_scan(self, project):
        with patch("google.cloud.dataplex_v1.DataScanServiceClient") as MockDataScanClient:
            mock_data_scan_client = MockDataScanClient.return_value

            results = run_dbt()
            assert len(results) == 1

            mock_data_scan_client.create_data_scan.assert_called_once()
            mock_data_scan_client.run_data_scan.assert_called_once()

            # Update the project to disable the data profile scan
            project_yml = os.path.join(project.project_root, "dbt_project.yml")
            config = yaml.safe_load(read_file(project_yml))
            config["models"]["+data_profile_scan"]["enabled"] = False
            write_config_file(config, project_yml)

            results = run_dbt()
            assert len(results) == 1
            mock_data_scan_client.delete_data_scan.assert_called_once()

            relation: BigQueryRelation = relation_from_name(project.adapter, MODEL_NAME)
            with get_connection(project.adapter) as conn:
                table = conn.handle.get_table(
                    project.adapter.connections.get_bq_table(
                        relation.database, relation.schema, relation.table
                    )
                )
                labels_to_be_created = list(ORIGINAL_LABELS.keys())
                assert set(table.labels.keys()) == set(labels_to_be_created)


class TestDataProfileScanWithIncrementalModel:
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "models": {
                "+labels": ORIGINAL_LABELS,
                "+data_profile_scan": {
                    "location": SCAN_LOCATION,
                    "scan_id": SCAN_ID,
                    "sampling_percent": 10,
                    "row_filter": "TRUE",
                },
            },
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {
            f"{MODEL_NAME}.sql": INCREMENTAL_MODEL_CONTENT,
            f"{MODEL_NAME}.yml": YAML_CONTENT,
        }

    def test_create_data_profile_scan(self, project):
        with patch("google.cloud.dataplex_v1.DataScanServiceClient") as MockDataScanClient:
            mock_data_scan_client = MockDataScanClient.return_value

            results = run_dbt()
            assert len(results) == 1

            mock_data_scan_client.create_data_scan.assert_called_once()
            mock_data_scan_client.run_data_scan.assert_called_once()

            def list_data_scans_mock(parent):
                mock_scan = MagicMock()
                mock_scan.name = SCAN_ID
                return [mock_scan]

            mock_data_scan_client.list_data_scans.side_effect = list_data_scans_mock

            results = run_dbt()
            assert len(results) == 1
            mock_data_scan_client.update_data_scan.assert_called_once()

            relation: BigQueryRelation = relation_from_name(project.adapter, MODEL_NAME)
            with get_connection(project.adapter) as conn:
                table = conn.handle.get_table(
                    project.adapter.connections.get_bq_table(
                        relation.database, relation.schema, relation.table
                    )
                )
                labels_to_be_created = PROFILE_SCAN_LABELS + list(ORIGINAL_LABELS.keys())
                assert set(table.labels.keys()) == set(labels_to_be_created)
