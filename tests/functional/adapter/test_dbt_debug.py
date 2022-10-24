import pytest
from dbt.tests.util import run_dbt
from dbt.tests.adapter.dbt_debug.test_dbt_debug import BaseDebug


class TestDebugBigQuery(BaseDebug):
    def test_ok_bigquery(self, project):
        run_dbt(["debug"])
        assert "ERROR" not in self.capsys.readouterr().out


class TestDebugProfileVariableBigQuery(TestDebugBigQuery):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            'config-version': 2,
            'profile': '{{ "te" ~ "st" }}'
        }