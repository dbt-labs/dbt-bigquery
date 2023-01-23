import pytest

from dbt.tests.adapter.incremental.test_incremental_on_schema_change import (
    BaseIncrementalOnSchemaChangeSetup,
    BaseIncrementalOnSchemaChange,
)

from dbt.tests.adapter.incremental.fixtures import (
    _MODELS__A,
    _MODELS__INCREMENTAL_SYNC_ALL_COLUMNS_TARGET,
)


class TestIncrementalOnSchemaChange(BaseIncrementalOnSchemaChange):
    pass


_MODELS__INCREMENTAL_SYNC_ALL_COLUMNS_DYNAMIC_INSERT_OVERWRITE = """
{{
    config(
        materialized='incremental',
        unique_key='id',
        on_schema_change='sync_all_columns',
        partition_by={
            "field": "id",
            "data_type": "int64",
            "range": {
                "start": 1,
                "end": 6,
                "interval": 1
            }
        },
        incremental_strategy='insert_overwrite'
    )
}}

WITH source_data AS (SELECT * FROM {{ ref('model_a') }} )

{% set string_type = 'string' %}

{% if is_incremental() %}

SELECT id, 
       cast(field1 as {{string_type}}) as field1, 
       cast(field3 as {{string_type}}) as field3, -- to validate new fields
       cast(field4 as {{string_type}}) AS field4 -- to validate new fields

FROM source_data WHERE id > _dbt_max_partition

{% else %}

select id, 
       cast(field1 as {{string_type}}) as field1, 
       cast(field2 as {{string_type}}) as field2

from source_data where id <= 3

{% endif %}
"""

_MODELS__INCREMENTAL_TIME_INGESTION_PARTITIONING = """

{{
    config(
        materialized="incremental",
        incremental_strategy='insert_overwrite',
        partition_by={
            "field": "date_hour",
            "data_type": "datetime",
            "granularity": "hour",
            "time_ingestion_partitioning": true
        }
    )
}}


with data as (

    {% if not is_incremental() %}

        select 1 as id, cast('2020-01-01 01:00:00' as datetime) as date_hour union all
        select 2 as id, cast('2020-01-01 01:00:00' as datetime) as date_hour union all
        select 3 as id, cast('2020-01-01 01:00:00' as datetime) as date_hour union all
        select 4 as id, cast('2020-01-01 01:00:00' as datetime) as date_hour

    {% else %}

        -- we want to overwrite the 4 records in the 2020-01-01 01:00:00 partition
        -- with the 2 records below, but add two more in the 2020-01-00 02:00:00 partition
        select 10 as id, cast('2020-01-01 01:00:00' as datetime) as date_hour union all
        select 20 as id, cast('2020-01-01 01:00:00' as datetime) as date_hour union all
        select 30 as id, cast('2020-01-01 02:00:00' as datetime) as date_hour union all
        select 40 as id, cast('2020-01-01 02:00:00' as datetime) as date_hour

    {% endif %}

)

select * from data
"""

_MODELS__INCREMENTAL_TIME_INGESTION_PARTITIONING_TARGET = """
{{
    config(
        materialized="incremental",
        partition_by={
            "field": "date_hour",
            "data_type": "datetime",
            "granularity": "hour",
            "time_ingestion_partitioning": true
        }
    )
}}

{% if not is_incremental() %}

    select 10 as id, cast('2020-01-01 01:00:00' as datetime) as date_hour union all
    select 30 as id, cast('2020-01-01 02:00:00' as datetime) as date_hour

{% else %}

    select 20 as id, cast('2020-01-01 01:00:00' as datetime) as date_hour union all
    select 40 as id, cast('2020-01-01 02:00:00' as datetime) as date_hour

{% endif %}
"""

class TestIncrementalOnSchemaChangeBigQuerySpecific(BaseIncrementalOnSchemaChangeSetup):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "model_a.sql": _MODELS__A,
            "incremental_sync_all_columns_dynamic_insert_overwrite.sql":
                _MODELS__INCREMENTAL_SYNC_ALL_COLUMNS_DYNAMIC_INSERT_OVERWRITE,
            "incremental_sync_all_columns_target.sql":
                _MODELS__INCREMENTAL_SYNC_ALL_COLUMNS_TARGET,
            "incremental_time_ingestion_partitioning.sql":
                _MODELS__INCREMENTAL_TIME_INGESTION_PARTITIONING,
            "incremental_time_ingestion_partitioning_target.sql":
                _MODELS__INCREMENTAL_TIME_INGESTION_PARTITIONING_TARGET,
        }
    
    def test_run_incremental_sync_all_columns_dynamic_insert_overwrite(self, project):
        select = 'model_a incremental_sync_all_columns_dynamic_insert_overwrite incremental_sync_all_columns_target'
        compare_source = 'incremental_sync_all_columns_dynamic_insert_overwrite'
        compare_target = 'incremental_sync_all_columns_target'
        self.run_twice_and_assert(select, compare_source, compare_target, project)
    
    # TODO: this test was added here, but it doesn't actually use 'on_schema_change'
    def test_run_incremental_time_ingestion_partitioning(self, project):
        select = 'model_a incremental_time_ingestion_partitioning incremental_time_ingestion_partitioning_target'
        compare_source = 'incremental_time_ingestion_partitioning'
        compare_target = 'incremental_time_ingestion_partitioning_target'
        self.run_twice_and_assert(select, compare_source, compare_target, project)
