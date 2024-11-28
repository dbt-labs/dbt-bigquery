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
MY_MATERIALIZED_VIEW = """
{{ config(
    materialized='materialized_view',
    partition_by={
        "field": "record_valid_date",
        "data_type": "datetime",
        "granularity": "day"
    },
    cluster_by=["id", "value"],
    enable_refresh=True,
    refresh_interval_minutes=60,
    max_staleness="INTERVAL 45 MINUTE",
    allow_non_incremental_definition=True
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
{{
  config(
    materialized = 'materialized_view',
    )
}}

select * from {{ ref('my_seed') }}
"""
