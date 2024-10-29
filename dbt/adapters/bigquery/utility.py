import json

import dbt_common.exceptions


def sql_escape(string):
    if not isinstance(string, str):
        raise dbt_common.exceptions.CompilationError(f"cannot escape a non-string: {string}")
    return json.dumps(string)[1:-1]
