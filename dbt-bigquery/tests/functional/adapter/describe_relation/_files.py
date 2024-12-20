MY_SEED = """
id,value,record_date
1,100,2023-01-01 12:00:00
2,200,2023-01-02 12:00:00
3,300,2023-01-02 12:00:00
""".strip()


MY_BASE_TABLE = """
{{ config(
    materialized='table',
    partition_by={
        "field": "record_date",
        "data_type": "datetime",
        "granularity": "day"
    },
    cluster_by=["id", "value"]
) }}
select
    id,
    value,
    record_date
from {{ ref('my_seed') }}
"""


MY_MATERIALIZED_VIEW = """
{{ config(
    materialized='materialized_view',
    partition_by={
        "field": "record_date",
        "data_type": "datetime",
        "granularity": "day"
    },
    cluster_by="id",
) }}
select
    id,
    value,
    record_date
from {{ ref('my_base_table') }}
"""


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
    record_date
from {{ ref('my_seed') }}
"""


MY_OTHER_MATERIALIZED_VIEW = """
{{ config(
    materialized='materialized_view',
    partition_by={
        "field": "value",
        "data_type": "int64",
        "range": {
            "start": 0,
            "end": 500,
            "interval": 50
        }
    },
    cluster_by="id",
    enable_refresh=False,
    refresh_interval_minutes=60
) }}
select
    id,
    value,
    record_date
from {{ ref('my_other_base_table') }}
"""
