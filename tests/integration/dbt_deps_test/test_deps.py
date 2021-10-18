from tests.integration.base import DBTIntegrationTest, use_profile
import os
import re
import yaml

import pytest


class TestDeps(DBTIntegrationTest):
    @property
    def schema(self):
        return "dbt_deps"

    @staticmethod
    def dir(value):
        return os.path.normpath(value)

    @property
    def models(self):
        return "models"

    @pytest.fixture(autouse=True)
    def capsys(self, capsys):
        self.capsys = capsys

    @use_profile("bigquery")
    def test_bigquery_ok(self):
        self.run_dbt(["deps"])
        self.assertNotIn("ERROR", self.capsys.readouterr().out)


class TestDepsProfileVariable(TestDeps):
    @property
    def project_config(self):
        return {"config-version": 2, "profile": '{{ "te" ~ "st" }}'}
