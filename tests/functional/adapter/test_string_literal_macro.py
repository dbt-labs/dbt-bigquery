import pytest
from dbt.tests.util import run_dbt


_MODEL_SQL = """
select {{ dbt.string_literal('my multiline
string') }} as test
"""


class TestStringLiteralQuoting:
    @pytest.fixture(scope="class")
    def models(self):
        return {"my_model.sql": _MODEL_SQL}

    def test_string_literal_quoting(self, project):
        run_dbt()
