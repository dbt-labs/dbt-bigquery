from dataclasses import dataclass

from dbt.adapters.base.relation import Policy


class BigQueryIncludePolicy(Policy):
    database: bool = True
    schema: bool = True
    identifier: bool = True


@dataclass
class BigQueryQuotePolicy(Policy):
    database: bool = True
    schema: bool = True
    identifier: bool = True
