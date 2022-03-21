import pytest
import os
import json

# Import the fuctional fixtures as a plugin
# Note: fixtures with session scope need to be local

pytest_plugins = ["dbt.tests.fixtures.project"]

# The profile dictionary, used to write out profiles.yml
@pytest.fixture(scope="class")
def dbt_profile_target(unique_schema):
    credentials_json_str = os.getenv('BIGQUERY_TEST_SERVICE_ACCOUNT_JSON').replace("'", '')
    credentials = json.loads(credentials_json_str)
    project_id = credentials.get('project_id')
    return {
        'type': 'bigquery',
        'method': 'service-account-json',
        'threads': 1,
        'project': project_id,
        'keyfile_json': credentials,
        'schema': unique_schema,
    }
