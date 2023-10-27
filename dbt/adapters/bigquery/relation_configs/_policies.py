from dataclasses import dataclass

from dbt.adapters.base.relation import Policy
from dbt.contracts.relation import ComponentName


QUOTE_CHARACTER = "`"


class BigQueryIncludePolicy(Policy):
    database: bool = True
    schema: bool = True
    identifier: bool = True


@dataclass
class BigQueryQuotePolicy(Policy):
    database: bool = True
    schema: bool = True
    identifier: bool = True


def render_part(component: ComponentName, value: str) -> str:
    if BigQueryQuotePolicy().get_part(component):
        return f"{QUOTE_CHARACTER}{value}{QUOTE_CHARACTER}"
    return value.lower()
