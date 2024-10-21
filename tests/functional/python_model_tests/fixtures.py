import os
import pytest
from dbt.tests.fixtures.project import write_project_files

ALT_DATABASE = os.getenv("BIGQUERY_TEST_ALT_DATABASE")

models__view_1_sql = """
select 1 as id
"""

models__python_model_py = """
def model(dbt, session):
    return dbt.ref("view_1")
"""


@pytest.fixture(scope="class")
def models():
    return {
        "view_1.sql": models__view_1_sql,
        "python_model.py": models__python_model_py,
    }


@pytest.fixture(scope="class")
def project_files(
    project_root,
    models,
):
    write_project_files(project_root, "models", models)
