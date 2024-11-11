from contextlib import contextmanager
from typing import Generator

from google.auth.exceptions import RefreshError
from google.cloud.exceptions import BadRequest, ClientError, Forbidden, NotFound

from dbt_common.exceptions import DbtDatabaseError, DbtRuntimeError

from dbt.adapters.bigquery.services.bigquery._config import logger, query_job_url


@contextmanager
def exception_handler() -> Generator:
    try:
        yield
    except BadRequest as e:
        _database_error(e, "Bad request while running query")
    except Forbidden as e:
        _database_error(e, "Access denied while running query")
    except NotFound as e:
        _database_error(e, "Not found while running query")
    except RefreshError as e:
        message = (
            "Unable to generate access token, if you're using "
            "impersonate_service_account, make sure your "
            'initial account has the "roles/'
            'iam.serviceAccountTokenCreator" role on the '
            "account you are trying to impersonate.\n\n"
            f"{str(e)}"
        )
        raise DbtRuntimeError(message)


def _database_error(error: ClientError, message: str) -> None:
    if hasattr(error, "query_job"):
        logger.error(query_job_url(error.query_job))
    raise DbtDatabaseError(message + "\n".join([item["message"] for item in error.errors]))
