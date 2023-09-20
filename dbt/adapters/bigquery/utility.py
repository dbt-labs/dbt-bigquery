from typing import Any, Optional


def bool_setting(value: Optional[Any] = None) -> Optional[bool]:
    if value is None:
        return None
    elif isinstance(value, bool):
        return value
    elif isinstance(value, str):
        if value.lower() in ["true", "false"]:
            return bool(value)
        else:
            raise ValueError(
                f"Invalid input, "
                f"expecting bool or str ex. (True, False, 'true', 'False'), recieved: {value}"
            )
    else:
        raise TypeError(
            f"Invalide type for bool evaluation, "
            f"expecting bool or str, recieved: {type(value)}"
        )
