from typing import List
import yaml

import pytest

from dbt.tests.util import run_dbt

from tests.functional.adapter.sources_tests import project_files


class TestSourceFreshness:
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

    @pytest.mark.parametrize(
        "source,max_loaded_at,expect_pass",
        [
            ("source_with_loaded_at_field", "2012-09-30 16:38:29", False),
            # ("source_without_loaded_at_field", "2012-09-30 16:38:29", False),
        ],
    )
    def test_source_freshness(self, project, source, max_loaded_at, expect_pass):
        """
        This test case addresses https://github.com/dbt-labs/dbt-bigquery/issues/1044

        The first scenario above passes as expected. The second scenario is not currently testable.
        `dbt source freshness` never returns because `loaded_at_field` is missing.
        The expected behavior is that it should at least return, likely with an error or warning message
        indicating that no `loaded_at_field` was provided for the source. Consult with Product and DX.
        """
        commands = ["source", "freshness", "--select", f"source:test_source.{source}"]
        results = self._run_dbt(project, commands, expect_pass=expect_pass)
        assert len(results) == 1
        result = results[0]
        assert result.max_loaded_at.strftime("%Y-%m-%d %H:%M:%S") == max_loaded_at
