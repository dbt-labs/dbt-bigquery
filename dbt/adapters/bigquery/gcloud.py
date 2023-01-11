from dbt.events import AdapterLogger
import dbt.exceptions
from dbt.clients.system import run_cmd

NOT_INSTALLED_MSG = """
dbt requires the gcloud SDK to be installed to authenticate with BigQuery.
Please download and install the SDK, or use a Service Account instead.

https://cloud.google.com/sdk/
"""

logger = AdapterLogger("BigQuery")


def gcloud_installed():
    try:
        run_cmd(".", ["gcloud", "--version"])
        return True
    except OSError as e:
        logger.debug(e)
        return False


def setup_default_credentials():
    if gcloud_installed():
        run_cmd(".", ["gcloud", "auth", "application-default", "login"])
    else:
        raise dbt.exceptions.DbtRuntimeError(NOT_INSTALLED_MSG)
