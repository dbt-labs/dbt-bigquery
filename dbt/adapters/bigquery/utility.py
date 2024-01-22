import json
from typing import Any, Optional

import dbt_common.exceptions


def bool_setting(value: Optional[Any] = None) -> Optional[bool]:
    if value is None:
        return None
    elif isinstance(value, bool):
        return value
    elif isinstance(value, str):
        # don't do bool(value) as that is equivalent to: len(value) > 0
        if value.lower() == "true":
            return True
        elif value.lower() == "false":
            return False
        else:
            raise ValueError(
                f"Invalid input, "
                f"expecting `bool` or `str` ex. (True, False, 'true', 'False'), received: {value}"
            )
    else:
        raise TypeError(
            f"Invalid type for bool evaluation, "
            f"expecting `bool` or `str`, received: {type(value)}"
        )


def float_setting(value: Optional[Any] = None) -> Optional[float]:
    if value is None:
        return None
    elif any(isinstance(value, i) for i in [int, float, str]):
        return float(value)
    else:
        raise TypeError(
            f"Invalid type for float evaluation, "
            f"expecting `int`, `float`, or `str`, received: {type(value)}"
        )


def sql_escape(string):
    if not isinstance(string, str):
        raise dbt_common.exceptions.CompilationError(f"cannot escape a non-string: {string}")
    return json.dumps(string)[1:-1]
