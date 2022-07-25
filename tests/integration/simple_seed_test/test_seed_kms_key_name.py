import json
import os

from google.oauth2 import service_account
from google.api_core.exceptions import AlreadyExists
from google.cloud import bigquery
from google.cloud import kms

from tests.integration.base import DBTIntegrationTest, use_profile


class GcpKmsAdapter:
    def __init__(self, service_account_info, location_id) -> None:
        self.location_id = location_id
        credentials = service_account.Credentials.from_service_account_info(
            info=service_account_info
        )
        self.project_id = credentials.project_id
        self.kms_client = kms.KeyManagementServiceClient.from_service_account_info(
            info=service_account_info
        )

    def get_or_create_key(self, key_ring_id, key_id):
        self._create_key_ring(name=key_ring_id)
        self._create_key_symmetric_encrypt_decrypt(
            key_ring_id=key_ring_id, key_id=key_id
        )
        return f"projects/{self.project_id}/locations/{self.location_id}/keyRings/{key_ring_id}/cryptoKeys/{key_id}"

    def _create_key_ring(self, name):
        location_name = f"projects/{self.project_id}/locations/{self.location_id}"
        key_ring = {}
        try:
            self.kms_client.create_key_ring(
                request={
                    "parent": location_name,
                    "key_ring_id": name,
                    "key_ring": key_ring,
                }
            )
        except AlreadyExists:
            pass

    def _create_key_symmetric_encrypt_decrypt(self, key_ring_id, key_id):
        # Build the parent key ring name.
        key_ring_name = self.kms_client.key_ring_path(
            self.project_id, self.location_id, key_ring_id
        )

        # Build the key.
        purpose = kms.CryptoKey.CryptoKeyPurpose.ENCRYPT_DECRYPT
        algorithm = (
            kms.CryptoKeyVersion.CryptoKeyVersionAlgorithm.GOOGLE_SYMMETRIC_ENCRYPTION
        )
        key = {
            "purpose": purpose,
            "version_template": {
                "algorithm": algorithm,
            },
        }

        # Call the API.
        try:
            self.kms_client.create_crypto_key(
                request={
                    "parent": key_ring_name,
                    "crypto_key_id": key_id,
                    "crypto_key": key,
                }
            )
        except AlreadyExists:
            pass


class TestSimpleSeedKmsKeyName(DBTIntegrationTest):
    def setUp(self):
        self.gcs_kms_adapter = GcpKmsAdapter(
            service_account_info=self.gcs_service_account_info, location_id="us"
        )
        self.kms_key_name = self.gcs_kms_adapter.get_or_create_key(
            key_ring_id="dbt-integration-test", key_id="dbt-integration-test-key"
        )
        return super().setUp()

    @property
    def schema(self):
        return "simple_seed"

    @property
    def project_id(self):
        project_id = (
            self.bigquery_profile()
            .get("test", {})
            .get("outputs", {})
            .get("default2", {})
            .get("project")
        )
        if project_id is None:
            raise Exception("unable to get gcp project")
        return project_id

    @property
    def gcs_service_account_info(self):
        credentials_json_str = os.getenv("BIGQUERY_TEST_SERVICE_ACCOUNT_JSON").replace(
            "'", ""
        )
        credentials_dict = json.loads(credentials_json_str)
        return credentials_dict

    @property
    def bigquery_client(self):
        credentials = service_account.Credentials.from_service_account_info(
            info=self.gcs_service_account_info
        )
        client = bigquery.Client(
            credentials=credentials, project=credentials.project_id
        )
        return client

    @property
    def project_config(self):
        return {
            "config-version": 2,
            "seed-paths": ["seeds-config"],
            "macro-paths": ["macros"],
            "seeds": {
                "+kms_key_name": self.kms_key_name,
                "test": {
                    "enabled": False,
                    "quote_columns": True,
                    "seed_kms": {
                        "enabled": True,
                    },
                },
            },
        }


class TestSimpleSeedKmsKeyNameBq(TestSimpleSeedKmsKeyName):
    @property
    def models(self):
        return "models-bq"

    @property
    def bigquery_table_metadata(self):
        table_id = f"{self.project_id}.{self.unique_schema()}.seed_kms"
        table = self.bigquery_client.get_table(table_id)
        return table

    @property
    def profile_config(self):
        return self.bigquery_profile()

    @use_profile("bigquery")
    def test_bigquery_simple_seed_with_kms_key_name_bigquery(self):
        results = self.run_dbt(["seed", "--show"])
        self.assertEqual(len(results), 1)
        self.assertIsNotNone(
            self.bigquery_table_metadata.encryption_configuration.kms_key_name
        )
