import pytest
from dbt.tests.adapter.simple_seed.fixtures import macros__schema_test
from dbt.tests.adapter.simple_seed.seeds import seeds__enabled_in_config_csv, seeds__tricky_csv
from dbt.tests.adapter.simple_seed.test_seed import SeedConfigBase
from dbt.tests.adapter.simple_seed.test_seed import BaseTestEmptySeed
from dbt.tests.adapter.utils.base_utils import run_dbt


_SEED_CONFIGS_CSV = """
seed_id,stuff
1,a
2,b
""".lstrip()

_SCHEMA_YML = """
version: 2
seeds:
- name: seed_enabled
  columns:
  - name: birthday
    tests:
    - column_type:
        type: STRING
  - name: seed_id
    tests:
    - column_type:
        type: FLOAT64

- name: seed_tricky
  columns:
  - name: seed_id
    tests:
    - column_type:
        type: INT64
  - name: seed_id_str
    tests:
    - column_type:
        type: STRING
  - name: a_bool
    tests:
    - column_type:
        type: BOOLEAN
  - name: looks_like_a_bool
    tests:
    - column_type:
        type: STRING
  - name: a_date
    tests:
    - column_type:
        type: DATETIME
  - name: looks_like_a_date
    tests:
    - column_type:
        type: STRING
  - name: relative
    tests:
    - column_type:
        type: STRING
  - name: weekday
    tests:
    - column_type:
        type: STRING

- name: seed_configs
  config:
    hours_to_expiration: 2
    labels:
      contains_pii: 'yes'
      contains_pie: 'no'
""".lstrip()


class TestSimpleSeedConfigs(SeedConfigBase):
    @pytest.fixture(scope="class")
    def schema(self):
        return "simple_seed"

    @pytest.fixture(scope="class")
    def seeds(self):
        return {
            "seed_enabled.csv": seeds__enabled_in_config_csv,
            "seed_tricky.csv": seeds__tricky_csv,
            "seed_configs.csv": _SEED_CONFIGS_CSV,
        }

    @pytest.fixture(scope="class")
    def macros(self):
        return {
            "schema_test.sql": macros__schema_test,
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {"models-bq.yml": _SCHEMA_YML}

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "config-version": 2,
            "seeds": {
                "test": {
                    "enabled": False,
                    "quote_columns": True,
                    "seed_enabled": {
                        "enabled": True,
                        "+column_types": self.seed_enabled_types(),
                    },
                    "seed_tricky": {
                        "enabled": True,
                        "+column_types": self.seed_tricky_types(),
                    },
                    "seed_configs": {
                        "enabled": True,
                    },
                },
            },
        }

    @staticmethod
    def seed_enabled_types():
        return {
            "seed_id": "FLOAT64",
            "birthday": "STRING",
        }

    @staticmethod
    def seed_tricky_types():
        return {
            "seed_id_str": "STRING",
            "looks_like_a_bool": "STRING",
            "looks_like_a_date": "STRING",
        }

    @staticmethod
    def table_labels():
        return {"contains_pii": "yes", "contains_pie": "no"}

    def test__bigquery_simple_seed_with_column_override_bigquery(self, project):
        seed_results = run_dbt(["seed"])
        assert len(seed_results) == 3
        test_results = run_dbt(["test"])
        assert len(test_results) == 10

    def test__bigquery_seed_table_with_labels_config_bigquery(self, project):
        seed_results = run_dbt(["seed"])
        assert len(seed_results) == 3
        with project.adapter.connection_named("_test"):
            client = project.adapter.connections.get_thread_connection().handle
            table_id = "{}.{}.{}".format(project.database, project.test_schema, "seed_configs")
            bq_table = client.get_table(table_id)

            assert bq_table.labels
            assert bq_table.labels == self.table_labels()
            assert bq_table.expires


class TestBigQueryEmptySeed(BaseTestEmptySeed):
    pass
