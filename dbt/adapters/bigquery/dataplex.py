from dataclasses import dataclass
import hashlib
from typing import Optional

from dbt.adapters.bigquery import BigQueryConnectionManager
from google.cloud import dataplex_v1
from google.protobuf import field_mask_pb2


@dataclass
class DataProfileScanSetting:
    location: str
    scan_id: Optional[str]

    project_id: str
    dataset_id: str
    table_id: str

    sampling_percent: Optional[float]
    row_filter: Optional[str]
    cron: Optional[str]

    def parent(self):
        return f"projects/{self.project_id}/locations/{self.location}"

    def data_scan_name(self):
        return f"{self.parent()}/dataScans/{self.scan_id}"


class DataProfileScan:
    def __init__(self, connections: BigQueryConnectionManager):
        self.connections = connections

    # If the label `dataplex-dp-published-*` is not assigned, we cannot view the results of the Data Profile Scan from BigQuery
    def _update_labels_with_data_profile_scan_labels(
        self,
        project_id: str,
        dataset_id: str,
        table_id: str,
        location: str,
        scan_id: str,
    ):
        table = self.connections.get_bq_table(project_id, dataset_id, table_id)
        original_labels = table.labels
        profile_scan_labels = {
            "dataplex-dp-published-scan": scan_id,
            "dataplex-dp-published-project": project_id,
            "dataplex-dp-published-location": location,
        }
        table.labels = {**original_labels, **profile_scan_labels}
        self.connections.get_thread_connection().handle.update_table(table, ["labels"])

    # scan_id must be unique within the project and no longer than 63 characters,
    # so generate an id that meets the constraints
    def _generate_unique_scan_id(self, dataset_id: str, table_id: str) -> str:
        md5 = hashlib.md5(f"{dataset_id}_{table_id}".encode("utf-8")).hexdigest()
        return f"dbt-{table_id.replace('_', '-')}-{md5}"[:63]

    def _create_or_update_data_profile_scan(
        self,
        client: dataplex_v1.DataScanServiceClient,
        scan_setting: DataProfileScanSetting,
    ):
        data_profile_spec = dataplex_v1.DataProfileSpec(
            sampling_percent=scan_setting.sampling_percent,
            row_filter=scan_setting.row_filter,
        )
        display_name = (
            f"Data Profile Scan for {scan_setting.table_id} in {scan_setting.dataset_id}"
        )
        description = f"This is a Data Profile Scan for {scan_setting.project_id}.{scan_setting.dataset_id}.{scan_setting.table_id}. Created by dbt."
        labels = {
            "managed_by": "dbt",
        }

        if scan_setting.cron:
            trigger = dataplex_v1.Trigger(
                schedule=dataplex_v1.Trigger.Schedule(cron=scan_setting.cron)
            )
        else:
            trigger = dataplex_v1.Trigger(on_demand=dataplex_v1.Trigger.OnDemand())
        execution_spec = dataplex_v1.DataScan.ExecutionSpec(trigger=trigger)

        if all(
            scan.name != scan_setting.data_scan_name()
            for scan in client.list_data_scans(parent=scan_setting.parent())
        ):
            data_scan = dataplex_v1.DataScan(
                data=dataplex_v1.DataSource(
                    resource=f"//bigquery.googleapis.com/projects/{scan_setting.project_id}/datasets/{scan_setting.dataset_id}/tables/{scan_setting.table_id}"
                ),
                data_profile_spec=data_profile_spec,
                execution_spec=execution_spec,
                display_name=display_name,
                description=description,
                labels=labels,
            )
            request = dataplex_v1.CreateDataScanRequest(
                parent=scan_setting.parent(),
                data_scan_id=scan_setting.scan_id,
                data_scan=data_scan,
            )
            client.create_data_scan(request=request).result()
        else:
            request = dataplex_v1.GetDataScanRequest(
                name=scan_setting.data_scan_name(),
            )
            data_scan = client.get_data_scan(request=request)

            data_scan.data_profile_spec = data_profile_spec
            data_scan.execution_spec = execution_spec
            data_scan.display_name = display_name
            data_scan.description = description
            data_scan.labels = labels

            update_mask = field_mask_pb2.FieldMask(
                paths=[
                    "data_profile_spec",
                    "execution_spec",
                    "display_name",
                    "description",
                    "labels",
                ]
            )
            request = dataplex_v1.UpdateDataScanRequest(
                data_scan=data_scan,
                update_mask=update_mask,
            )
            client.update_data_scan(request=request).result()

    def create_or_update_data_profile_scan(self, config):
        project_id = config.get("database")
        dataset_id = config.get("schema")
        table_id = config.get("name")

        data_profile_config = config.get("config").get("data_profile_scan", {})

        # Skip if data_profile_scan is not configured
        if not data_profile_config:
            return None

        client = dataplex_v1.DataScanServiceClient()
        scan_setting = DataProfileScanSetting(
            location=data_profile_config["location"],
            scan_id=data_profile_config.get(
                "scan_id", self._generate_unique_scan_id(dataset_id, table_id)
            ),
            project_id=project_id,
            dataset_id=dataset_id,
            table_id=table_id,
            sampling_percent=data_profile_config.get("sampling_percent", None),
            row_filter=data_profile_config.get("row_filter", None),
            cron=data_profile_config.get("cron", None),
        )

        # Delete existing data profile scan if it is disabled
        if not data_profile_config.get("enabled", True):
            client.delete_data_scan(name=scan_setting.data_scan_name())
            return None

        self._create_or_update_data_profile_scan(client, scan_setting)

        if not scan_setting.cron:
            client.run_data_scan(
                request=dataplex_v1.RunDataScanRequest(name=scan_setting.data_scan_name())
            )

        self._update_labels_with_data_profile_scan_labels(
            project_id, dataset_id, table_id, scan_setting.location, scan_setting.scan_id
        )
