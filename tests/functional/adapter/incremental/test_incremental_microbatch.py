import pytest

from dbt.tests.adapter.incremental.test_incremental_microbatch import (
    BaseMicrobatch,
)


_microbatch_model_no_unique_id_sql = """
{{ config(
    materialized='incremental',
    incremental_strategy='microbatch',
    partition_by={
      "field": "event_time",
      "data_type": "timestamp",
      "granularity": "day"
    },
    event_time='event_time',
    batch_size='day',
    begin=modules.datetime.datetime(2020, 1, 1, 0, 0, 0)
    )
}}
select * from {{ ref('input_model') }}
"""


class TestBigQueryMicrobatch(BaseMicrobatch):
    @pytest.fixture(scope="class")
    def microbatch_model_sql(self) -> str:
        return _microbatch_model_no_unique_id_sql
