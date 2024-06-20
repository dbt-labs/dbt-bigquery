# flake8: noqa
# ignores the special characters in the descripton check

MY_SEED = """
id,value,record_valid_date
1,100,2023-01-01 00:00:00
2,200,2023-01-02 00:00:00
3,300,2023-01-02 00:00:00
""".strip()


MY_BASE_TABLE = """
{{ config(
    materialized='table',
    partition_by={
        "field": "record_valid_date",
        "data_type": "datetime",
        "granularity": "day"
    },
    cluster_by=["id", "value"]
) }}
select
    id,
    value,
    record_valid_date
from {{ ref('my_seed') }}
"""


# the whitespace to the left on partition matters here
# this should test all possible config (skip KMS key since it needs to exist)
MY_MATERIALIZED_VIEW = """
{{ config(
    materialized="materialized_view",
    partition_by={
        "field": "record_valid_date",
        "data_type": "datetime",
        "granularity": "day"
    },
    cluster_by=["id", "value"],
    enable_refresh=True,
    refresh_interval_minutes=60,
    hours_to_expiration=24,
    max_staleness="INTERVAL '0-0 0 0:45:0' YEAR TO SECOND",
    allow_non_incremental_definition=True,
    description="<Th!s is /a 'string' with specia\ ch@rs.",
    labels={"name": "tony_stark", "alias": "ironman"}
) }}
select
    id,
    value,
    record_valid_date
from {{ ref('my_base_table') }}
"""


# the whitespace to the left on partition matters here
MY_OTHER_BASE_TABLE = """
{{ config(
    materialized='table',
    partition_by={
        "field": "value",
        "data_type": "int64",
        "range": {
            "start": 0,
            "end": 500,
            "interval": 50
        }
    },
    cluster_by=["id", "value"]
) }}
select
    id,
    value,
    record_valid_date
from {{ ref('my_seed') }}
"""


MY_MINIMAL_MATERIALIZED_VIEW = """
{{ config(
    materialized='materialized_view'
) }}

select * from {{ ref('my_seed') }}
"""


MY_MATERIALIZED_VIEW_NON_INCREMENTAL = """
{{ config(
    max_staleness="INTERVAL '0-0 0 0:45:0' YEAR TO SECOND",
    allow_non_incremental_definition=True,
    materialized='materialized_view'
) }}
select
    id,
    value,
    record_valid_date
from {{ ref('my_base_table') }}
"""


MY_MATERIALIZED_VIEW_REFRESH_CONFIG = """
{{ config(
    enable_refresh=True,
    refresh_interval_minutes=60,
    max_staleness="INTERVAL '0-0 0 0:45:0' YEAR TO SECOND",
    allow_non_incremental_definition=True,
    materialized='materialized_view'
) }}
select
    id,
    value,
    record_valid_date
from {{ ref('my_base_table') }}
"""
