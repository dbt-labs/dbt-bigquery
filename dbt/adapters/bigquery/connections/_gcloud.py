from dbt_common.clients.system import run_cmd
from dbt_common.exceptions import DbtRuntimeError
from dbt.adapters.events.logging import AdapterLogger


_NOT_INSTALLED_MSG = """
dbt requires the gcloud SDK to be installed to authenticate with BigQuery.
Please download and install the SDK, or use a Service Account instead.

https://cloud.google.com/sdk/
"""


_logger = AdapterLogger("BigQuery")


def setup_default_credentials():
    if _gcloud_installed():
        run_cmd(".", ["gcloud", "auth", "application-default", "login"])
    else:
        raise DbtRuntimeError(_NOT_INSTALLED_MSG)


def _gcloud_installed():
    try:
        run_cmd(".", ["gcloud", "--version"])
        return True
    except OSError as e:
        _logger.debug(e)
        return False
