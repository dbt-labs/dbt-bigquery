import base64
import binascii
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


def is_base64(s: str | bytes) -> bool:
    """
    Checks if the given string or bytes object is valid Base64 encoded.

    Args:
        s: The string or bytes object to check.

    Returns:
        True if the input is valid Base64, False otherwise.
    """

    if isinstance(s, str):
        # For strings, ensure they consist only of valid Base64 characters
        if not s.isascii():
            return False
        # Convert to bytes for decoding
        s = s.encode("ascii")

    try:
        # Use the 'validate' parameter to enforce strict Base64 decoding rules
        base64.b64decode(s, validate=True)
        return True
    except TypeError:
        return False
    except binascii.Error:  # Catch specific errors from the base64 module
        return False


def base64_to_string(b):
    return base64.b64decode(b).decode("utf-8")


def string_to_base64(s):
    return base64.b64encode(s.encode("utf-8"))
