from typing import Optional, Tuple

import dbt.exceptions
from dbt.clients.system import run_cmd
from dbt.events import AdapterLogger

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


def setup_default_credentials(scopes: Optional[Tuple[str, ...]] = None):
    scopes_args = ["--scopes", ",".join(scopes)] if scopes else []
    if gcloud_installed():
        run_cmd(".", ["gcloud", "auth", "application-default", "login"] + scopes_args)
    else:
        raise dbt.exceptions.RuntimeException(NOT_INSTALLED_MSG)
