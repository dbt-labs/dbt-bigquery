import pytest
import os
import json

# Import the fuctional fixtures as a plugin
# Note: fixtures with session scope need to be local

pytest_plugins = ["dbt.tests.fixtures.project"]


def pytest_addoption(parser):
    parser.addoption("--profile", action="store", default="oauth", type=str)


@pytest.fixture(scope="session")
def dbt_profile_target(request):
    profile_type = request.config.getoption("--profile")
    if profile_type == "oauth":
        target = oauth_target()
    elif profile_type == "service_account":
        target = service_account_target()
    else:
        raise ValueError(f"Invalid profile type '{profile_type}'")
    return target


def oauth_target():
    return {
        'type': 'bigquery',
        'method': 'oauth',
        'threads': 1,
        'job_retries': 2,
    }


def service_account_target():
    credentials_json_str = os.getenv('BIGQUERY_TEST_SERVICE_ACCOUNT_JSON').replace("'", '')
    credentials = json.loads(credentials_json_str)
    project_id = credentials.get('project_id')
    return {
        'type': 'bigquery',
        'method': 'service-account-json',
        'threads': 1,
        'job_retries': 2,
        'project': project_id,
        'keyfile_json': credentials,
        # following 3 for python model
        'dataproc_region': os.getenv("DATAPROC_REGION"),
        'dataproc_cluster_name': os.getenv("DATAPROC_CLUSTER_NAME"), # only needed for cluster submission method
        'gcs_bucket': os.getenv("GCS_BUCKET")
    }
