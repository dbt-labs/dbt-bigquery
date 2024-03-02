from datetime import datetime, timedelta, timezone
from typing import List
import yaml

from dbt.tests.util import run_dbt
import pytest

from tests.functional.adapter.sources_tests import project_files


class TestSourceFreshness:
    """
    This test case addresses https://github.com/dbt-labs/dbt-bigquery/issues/1044

    The first scenario above passes as expected. The second scenario is not currently testable.
    `dbt source freshness` never returns because `loaded_at_field` is missing.
    The expected behavior is that it should at least return, likely with an error or warning message
    indicating that no `loaded_at_field` was provided for the source. Consult with Product and DX.
    """

    @pytest.fixture(scope="class")
    def seeds(self):
        # these are the same files, but the schema doesn't specify the loaded at field for the second one
        return {
            "source_with_loaded_at_field.csv": project_files.SEED__SOURCE__CSV,
            "source_without_loaded_at_field.csv": project_files.SEED__SOURCE__CSV,
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "schema.yml": project_files.SCHEMA__YML,
            "with_loaded_at_field.sql": project_files.MODEL__WITH_LOADED_AT_FIELD__SQL,
            "without_loaded_at_field.sql": project_files.MODEL__WITHOUT_LOADED_AT_FIELD__SQL,
        }

    @pytest.fixture(scope="class", autouse=True)
    def setup(self, project):
        self._run_dbt(project, ["seed"])

    @staticmethod
    def _run_dbt(project, commands: List[str], *args, **kwargs):
        vars_dict = {
            "test_run_schema": project.test_schema,
            "test_loaded_at": project.adapter.quote("updated_at"),
        }
        commands.extend(["--vars", yaml.safe_dump(vars_dict)])
        return run_dbt(commands, *args, **kwargs)

    def test_source_freshness_with_loaded_at_field(self, project):
        commands = [
            "source",
            "freshness",
            "--select",
            "source:test_source.source_with_loaded_at_field",
        ]
        results = self._run_dbt(project, commands, expect_pass=False)
        assert len(results) == 1
        result = results[0]
        assert result.max_loaded_at.strftime("%Y-%m-%d %H:%M:%S") == "2012-09-30 16:38:29"

    def test_source_freshness_without_loaded_at_field(self, project):
        commands = [
            "source",
            "freshness",
            "--select",
            "source:test_source.source_without_loaded_at_field",
        ]
        results = self._run_dbt(project, commands, expect_pass=True)
        assert len(results) == 1
        result = results[0]
        actual = result.max_loaded_at
        estimated = datetime.now(tz=timezone.utc)
        assert estimated - actual < timedelta(seconds=5)
