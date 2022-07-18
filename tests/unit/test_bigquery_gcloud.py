from unittest.mock import patch, call

import dbt.clients
import pytest

from dbt.adapters.bigquery.gcloud import setup_default_credentials


@pytest.fixture(scope="function")
def run_cmd():
    with patch.object(dbt.adapters.bigquery.gcloud, "run_cmd") as run_cmd_mock:
        yield run_cmd_mock


def test_setup_default_credentials_no_scopes(run_cmd):
    setup_default_credentials()
    assert run_cmd.call_args_list == [
        call(".", ["gcloud", "--version"]),
        call(".", ["gcloud", "auth", "application-default", "login"]),
    ]


def test_setup_default_credentials_with_scopes(run_cmd):
    setup_default_credentials(scopes=["scope1", "scope2"])
    assert run_cmd.call_args_list == [
        call(".", ["gcloud", "--version"]),
        call(
            ".",
            [
                "gcloud",
                "auth",
                "application-default",
                "login",
                "--scopes",
                "scope1,scope2",
            ],
        ),
    ]
