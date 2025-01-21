merge_range_sql = """
{{
    config(
        materialized="incremental",
        unique_key="id",
        cluster_by="id",
        partition_by={
            "field": "id",
            "data_type": "int64",
            "range": {
                "start": 1,
                "end": 10,
                "interval": 1
            }
        }
    )
}}


with data as (

    {% if not is_incremental() %}

        select 1 as id, cast('2020-01-01' as datetime) as date_time union all
        select 2 as id, cast('2020-01-01' as datetime) as date_time union all
        select 3 as id, cast('2020-01-01' as datetime) as date_time union all
        select 4 as id, cast('2020-01-01' as datetime) as date_time

    {% else %}

        select 1 as id, cast('2020-01-01' as datetime) as date_time union all
        select 2 as id, cast('2020-01-01' as datetime) as date_time union all
        select 3 as id, cast('2020-01-01' as datetime) as date_time union all
        select 4 as id, cast('2020-01-02' as datetime) as date_time union all
        select 5 as id, cast('2020-01-02' as datetime) as date_time union all
        select 6 as id, cast('2020-01-02' as datetime) as date_time

    {% endif %}

)

select * from data

{% if is_incremental() %}
where id >= (select max(id) from {{ this }})
{% endif %}
""".lstrip()

merge_time_sql = """
{{
    config(
        materialized="incremental",
        unique_key="id",
        cluster_by="id",
        partition_by={
            "field": "date_time",
            "data_type": "dateTime"
        }
    )
}}



with data as (

    {% if not is_incremental() %}

        select 1 as id, cast('2020-01-01' as datetime) as date_time union all
        select 2 as id, cast('2020-01-01' as datetime) as date_time union all
        select 3 as id, cast('2020-01-01' as datetime) as date_time union all
        select 4 as id, cast('2020-01-01' as datetime) as date_time

    {% else %}

        select 1 as id, cast('2020-01-01' as datetime) as date_time union all
        select 2 as id, cast('2020-01-01' as datetime) as date_time union all
        select 3 as id, cast('2020-01-01' as datetime) as date_time union all
        select 4 as id, cast('2020-01-02' as datetime) as date_time union all
        select 5 as id, cast('2020-01-02' as datetime) as date_time union all
        select 6 as id, cast('2020-01-02' as datetime) as date_time

    {% endif %}

)

select * from data

{% if is_incremental() %}
where date_time > (select max(date_time) from {{ this }})
{% endif %}
""".lstrip()

merge_time_with_require_partition_sql = """
{{
    config(
        materialized="incremental",
        unique_key="id",
        cluster_by="id",
        partition_by={
            "field": "date_time",
            "data_type": "dateTime"
        },
        post_hook="
            create or replace view `{{ schema }}.incremental_merge_time_with_require_partition_view`
            as select * from {{ this }} where date_time is null or date_time is not null
        ",
        require_partition_filter=true
    )
}}



with data as (

    {% if not is_incremental() %}

        select 1 as id, cast('2020-01-01' as datetime) as date_time union all
        select 2 as id, cast('2020-01-01' as datetime) as date_time union all
        select 3 as id, cast('2020-01-01' as datetime) as date_time union all
        select 4 as id, cast('2020-01-01' as datetime) as date_time

    {% else %}

        select 1 as id, cast('2020-01-01' as datetime) as date_time union all
        select 2 as id, cast('2020-01-01' as datetime) as date_time union all
        select 3 as id, cast('2020-01-01' as datetime) as date_time union all
        select 4 as id, cast('2020-01-02' as datetime) as date_time union all
        select 5 as id, cast('2020-01-02' as datetime) as date_time union all
        select 6 as id, cast('2020-01-02' as datetime) as date_time

    {% endif %}

)

select * from data

{% if is_incremental() %}
where date_time > (
    select max(date_time)
    from {{ this }}
    where (
        date_time is null
        or date_time is not null
    )
)

{% endif %}
""".lstrip()

overwrite_date_sql = """
{{
    config(
        materialized="incremental",
        incremental_strategy='insert_overwrite',
        cluster_by="id",
        partition_by={
            "field": "date_day",
            "data_type": "date"
        }
    )
}}


with data as (

    {% if not is_incremental() %}

        select 1 as id, cast('2020-01-01' as date) as date_day union all
        select 2 as id, cast('2020-01-01' as date) as date_day union all
        select 3 as id, cast('2020-01-01' as date) as date_day union all
        select 4 as id, cast('2020-01-01' as date) as date_day

    {% else %}

        -- we want to overwrite the 4 records in the 2020-01-01 partition
        -- with the 2 records below, but add two more in the 2020-01-02 partition
        select 10 as id, cast('2020-01-01' as date) as date_day union all
        select 20 as id, cast('2020-01-01' as date) as date_day union all
        select 30 as id, cast('2020-01-02' as date) as date_day union all
        select 40 as id, cast('2020-01-02' as date) as date_day

    {% endif %}

)

select * from data

{% if is_incremental() %}
where date_day >= _dbt_max_partition
{% endif %}
""".lstrip()

overwrite_day_sql = """
{{
    config(
        materialized="incremental",
        incremental_strategy='insert_overwrite',
        cluster_by="id",
        partition_by={
            "field": "date_time",
            "data_type": "datetime"
        }
    )
}}


with data as (

    {% if not is_incremental() %}

        select 1 as id, cast('2020-01-01' as datetime) as date_time union all
        select 2 as id, cast('2020-01-01' as datetime) as date_time union all
        select 3 as id, cast('2020-01-01' as datetime) as date_time union all
        select 4 as id, cast('2020-01-01' as datetime) as date_time

    {% else %}

        -- we want to overwrite the 4 records in the 2020-01-01 partition
        -- with the 2 records below, but add two more in the 2020-01-02 partition
        select 10 as id, cast('2020-01-01' as datetime) as date_time union all
        select 20 as id, cast('2020-01-01' as datetime) as date_time union all
        select 30 as id, cast('2020-01-02' as datetime) as date_time union all
        select 40 as id, cast('2020-01-02' as datetime) as date_time

    {% endif %}

)

select * from data

{% if is_incremental() %}
where date_time >= _dbt_max_partition
{% endif %}
""".lstrip()

overwrite_day_with_copy_partitions_sql = """
{{
    config(
        materialized="incremental",
        incremental_strategy='insert_overwrite',
        cluster_by="id",
        partition_by={
            "field": "date_time",
            "data_type": "datetime",
            "copy_partitions": true
        }
    )
}}


with data as (

    {% if not is_incremental() %}

        select 1 as id, cast('2020-01-01' as datetime) as date_time union all
        select 2 as id, cast('2020-01-01' as datetime) as date_time union all
        select 3 as id, cast('2020-01-01' as datetime) as date_time union all
        select 4 as id, cast('2020-01-01' as datetime) as date_time

    {% else %}

        -- we want to overwrite the 4 records in the 2020-01-01 partition
        -- with the 2 records below, but add two more in the 2020-01-02 partition
        select 10 as id, cast('2020-01-01' as datetime) as date_time union all
        select 20 as id, cast('2020-01-01' as datetime) as date_time union all
        select 30 as id, cast('2020-01-02' as datetime) as date_time union all
        select 40 as id, cast('2020-01-02' as datetime) as date_time

    {% endif %}

)

select * from data

{% if is_incremental() %}
where date_time >= _dbt_max_partition
{% endif %}
""".lstrip()

overwrite_day_with_time_partition_datetime_sql = """
{{
    config(
        materialized="incremental",
        incremental_strategy='insert_overwrite',
        cluster_by="id",
        partition_by={
            "field": "date_day",
            "data_type": "date",
            "time_ingestion_partitioning": true
        }
    )
}}


with data as (

    {% if not is_incremental() %}

        select 1 as id, cast('2020-01-01' as date) as date_day union all
        select 2 as id, cast('2020-01-01' as date) as date_day union all
        select 3 as id, cast('2020-01-01' as date) as date_day union all
        select 4 as id, cast('2020-01-01' as date) as date_day

    {% else %}

        -- we want to overwrite the 4 records in the 2020-01-01 partition
        -- with the 2 records below, but add two more in the 2020-01-02 partition
        select 10 as id, cast('2020-01-01' as date) as date_day union all
        select 20 as id, cast('2020-01-01' as date) as date_day union all
        select 30 as id, cast('2020-01-02' as date) as date_day union all
        select 40 as id, cast('2020-01-02' as date) as date_day

    {% endif %}

)

select * from data

{% if is_incremental() %}
where date_day >= '2020-01-01'
{% endif %}
""".lstrip()

overwrite_partitions_sql = """
{{
    config(
        materialized="incremental",
        incremental_strategy='insert_overwrite',
        cluster_by="id",
        partitions=["'2020-01-01'","'2020-01-02'"],
        partition_by={
            "field": "date_day",
            "data_type": "date"
        }
    )
}}


with data as (

    {% if not is_incremental() %}

        select 1 as id, cast('2020-01-01' as date) as date_day union all
        select 2 as id, cast('2020-01-01' as date) as date_day union all
        select 3 as id, cast('2020-01-01' as date) as date_day union all
        select 4 as id, cast('2020-01-01' as date) as date_day

    {% else %}

        -- we want to overwrite the 4 records in the 2020-01-01 partition
        -- with the 2 records below, but add two more in the 2020-01-02 partition
        select 10 as id, cast('2020-01-01' as date) as date_day union all
        select 20 as id, cast('2020-01-01' as date) as date_day union all
        select 30 as id, cast('2020-01-02' as date) as date_day union all
        select 40 as id, cast('2020-01-02' as date) as date_day

    {% endif %}

)

select * from data

{% if is_incremental() %}
where date_day in ({{ config.get("partitions") | join(",") }})
{% endif %}
-- Test comment to prevent recurrence of https://github.com/dbt-labs/dbt-bigquery/issues/896
""".lstrip()

overwrite_range_sql = """
{{
    config(
        materialized="incremental",
        incremental_strategy='insert_overwrite',
        cluster_by="id",
        partition_by={
            "field": "date_int",
            "data_type": "int64",
            "range": {
                "start": 20200101,
                "end": 20200110,
                "interval": 1
            }
        }
    )
}}


with data as (

    {% if not is_incremental() %}

        select 1 as id, 20200101 as date_int union all
        select 2 as id, 20200101 as date_int union all
        select 3 as id, 20200101 as date_int union all
        select 4 as id, 20200101 as date_int

    {% else %}

        -- we want to overwrite the 4 records in the 20200101 partition
        -- with the 2 records below, but add two more in the 20200102 partition
        select 10 as id, 20200101 as date_int union all
        select 20 as id, 20200101 as date_int union all
        select 30 as id, 20200102 as date_int union all
        select 40 as id, 20200102 as date_int

    {% endif %}

)

select * from data

{% if is_incremental() %}
where date_int >= _dbt_max_partition
{% endif %}
""".lstrip()

overwrite_time_sql = """
{{
    config(
        materialized="incremental",
        incremental_strategy='insert_overwrite',
        cluster_by="id",
        partition_by={
            "field": "date_hour",
            "data_type": "datetime",
            "granularity": "hour"
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

{% if is_incremental() %}
where date_hour >= _dbt_max_partition
{% endif %}
""".lstrip()

overwrite_day_with_time_ingestion_sql = """
{{
    config(
        materialized="incremental",
        incremental_strategy='insert_overwrite',
        cluster_by="id",
        partition_by={
            "field": "date_time",
            "data_type": "datetime",
            "time_ingestion_partitioning": true
        },
        require_partition_filter=true
    )
}}


{%- call set_sql_header(config) %}
 CREATE TEMP FUNCTION asDateTime(date STRING) AS (
   cast(date as datetime)
 );
{%- endcall %}

with data as (

    {% if not is_incremental() %}

        select 1 as id, asDateTime('2020-01-01') as date_time union all
        select 2 as id, asDateTime('2020-01-01') as date_time union all
        select 3 as id, asDateTime('2020-01-01') as date_time union all
        select 4 as id, asDateTime('2020-01-01') as date_time

    {% else %}

        -- we want to overwrite the 4 records in the 2020-01-01 partition
        -- with the 2 records below, but add two more in the 2020-01-02 partition
        select 10 as id, asDateTime('2020-01-01') as date_time union all
        select 20 as id, asDateTime('2020-01-01') as date_time union all
        select 30 as id, cast('2020-01-02' as datetime) as date_time union all
        select 40 as id, cast('2020-01-02' as datetime) as date_time

    {% endif %}

)

select * from data

{% if is_incremental() %}
where date_time > '2020-01-01'
{% endif %}
""".lstrip()

overwrite_static_day_sql = """
{% set partitions_to_replace = [
  "'2020-01-01'",
  "'2020-01-02'",
] %}

{{
    config(
        materialized="incremental",
        incremental_strategy="insert_overwrite",
        cluster_by="id",
        partition_by={
            "field": "date_time",
            "data_type": "datetime",
            "granularity": "day"
        },
        partitions=partitions_to_replace,
        on_schema_change="sync_all_columns"
    )
}}


with data as (

    {% if not is_incremental() %}

        select 1 as id, cast('2020-01-01' as datetime) as date_time union all
        select 2 as id, cast('2020-01-01' as datetime) as date_time union all
        select 3 as id, cast('2020-01-01' as datetime) as date_time union all
        select 4 as id, cast('2020-01-01' as datetime) as date_time

    {% else %}

        -- we want to overwrite the 4 records in the 2020-01-01 partition
        -- with the 2 records below, but add two more in the 2020-01-02 partition
        select 10 as id, cast('2020-01-01' as datetime) as date_time union all
        select 20 as id, cast('2020-01-01' as datetime) as date_time union all
        select 30 as id, cast('2020-01-02' as datetime) as date_time union all
        select 40 as id, cast('2020-01-02' as datetime) as date_time

    {% endif %}

)

select * from data
""".lstrip()

microbatch_model_no_unique_id_sql = """
{{ config(
    materialized='incremental',
    incremental_strategy='microbatch',
    partition_by={
      'field': 'event_time',
      'data_type': 'timestamp',
      'granularity': 'day'
    },
    event_time='event_time',
    batch_size='day',
    begin=modules.datetime.datetime(2020, 1, 1, 0, 0, 0)
    )
}}
select id, cast(event_time as timestamp) as event_time from {{ ref('input_model') }}
"""

microbatch_input_sql = """
{{ config(materialized='table', event_time='event_time') }}
select 1 as id, TIMESTAMP '2020-01-01 00:00:00-0' as event_time
union all
select 2 as id, TIMESTAMP '2020-01-02 00:00:00-0' as event_time
union all
select 3 as id, TIMESTAMP '2020-01-03 00:00:00-0' as event_time
"""

microbatch_input_event_time_date_sql = """
{{ config(materialized='table', event_time='event_time') }}
select 1 as id, DATE '2020-01-01' as event_time
union all
select 2 as id, DATE '2020-01-02' as event_time
union all
select 3 as id, DATE '2020-01-03' as event_time
"""

microbatch_input_event_time_datetime_sql = """
{{ config(materialized='table', event_time='event_time') }}
select 1 as id, DATETIME '2020-01-01' as event_time
union all
select 2 as id, DATETIME '2020-01-02' as event_time
union all
select 3 as id, DATETIME '2020-01-03' as event_time
"""

microbatch_model_no_partition_by_sql = """
{{ config(
    materialized='incremental',
    incremental_strategy='microbatch',
    event_time='event_time',
    batch_size='day',
    begin=modules.datetime.datetime(2020, 1, 1, 0, 0, 0)
    )
}}
select * from {{ ref('input_model') }}
"""


microbatch_model_invalid_partition_by_sql = """
{{ config(
    materialized='incremental',
    incremental_strategy='microbatch',
    event_time='event_time',
    batch_size='day',
    begin=modules.datetime.datetime(2020, 1, 1, 0, 0, 0),
    partition_by={
      'field': 'event_time',
      'data_type': 'timestamp',
      'granularity': 'hour'
    }
    )
}}
select * from {{ ref('input_model') }}
"""

microbatch_model_no_unique_id_copy_partitions_sql = """
{{ config(
    materialized='incremental',
    incremental_strategy='microbatch',
    partition_by={
      'field': 'event_time',
      'data_type': 'timestamp',
      'granularity': 'day',
      'copy_partitions': true
    },
    event_time='event_time',
    batch_size='day',
    begin=modules.datetime.datetime(2020, 1, 1, 0, 0, 0)
    )
}}
select * from {{ ref('input_model') }}
"""

microbatch_model_no_unique_id_copy_partitions_sql = """
{{ config(
    materialized='incremental',
    incremental_strategy='microbatch',
    partition_by={
      'field': 'event_time',
      'data_type': 'timestamp',
      'granularity': 'day',
      'copy_partitions': true
    },
    event_time='event_time',
    batch_size='day',
    begin=modules.datetime.datetime(2020, 1, 1, 0, 0, 0)
    )
}}
select * from {{ ref('input_model') }}
"""

overwrite_static_day_delete_and_insert_sub_strategy_sql = """
{% set partitions_to_replace = [
  "'2020-01-01'",
  "'2020-01-02'",
] %}

{{
    config(
        materialized="incremental",
        incremental_strategy="insert_overwrite",
        incremental_substrategy='delete+insert',
        cluster_by="id",
        partition_by={
            "field": "date_time",
            "data_type": "datetime",
            "granularity": "day"
        },
        partitions=partitions_to_replace,
        on_schema_change="sync_all_columns"
    )
}}


with data as (

    {% if not is_incremental() %}

        select 1 as id, cast('2020-01-01' as datetime) as date_time union all
        select 2 as id, cast('2020-01-01' as datetime) as date_time union all
        select 3 as id, cast('2020-01-01' as datetime) as date_time union all
        select 4 as id, cast('2020-01-01' as datetime) as date_time

    {% else %}

        -- we want to overwrite the 4 records in the 2020-01-01 partition
        -- with the 2 records below, but add two more in the 2020-01-02 partition
        select 10 as id, cast('2020-01-01' as datetime) as date_time union all
        select 20 as id, cast('2020-01-01' as datetime) as date_time union all
        select 30 as id, cast('2020-01-02' as datetime) as date_time union all
        select 40 as id, cast('2020-01-02' as datetime) as date_time

    {% endif %}

)

select * from data
""".lstrip()

overwrite_static_day_commit_delete_and_insert_sub_strategy_sql = """
{% set partitions_to_replace = [
  "'2020-01-01'",
  "'2020-01-02'",
] %}

{{
    config(
        materialized="incremental",
        incremental_strategy="insert_overwrite",
        incremental_substrategy='commit+delete+insert',
        cluster_by="id",
        partition_by={
            "field": "date_time",
            "data_type": "datetime",
            "granularity": "day"
        },
        partitions=partitions_to_replace,
        on_schema_change="sync_all_columns"
    )
}}


with data as (

    {% if not is_incremental() %}

        select 1 as id, cast('2020-01-01' as datetime) as date_time union all
        select 2 as id, cast('2020-01-01' as datetime) as date_time union all
        select 3 as id, cast('2020-01-01' as datetime) as date_time union all
        select 4 as id, cast('2020-01-01' as datetime) as date_time

    {% else %}

        -- we want to overwrite the 4 records in the 2020-01-01 partition
        -- with the 2 records below, but add two more in the 2020-01-02 partition
        select 10 as id, cast('2020-01-01' as datetime) as date_time union all
        select 20 as id, cast('2020-01-01' as datetime) as date_time union all
        select 30 as id, cast('2020-01-02' as datetime) as date_time union all
        select 40 as id, cast('2020-01-02' as datetime) as date_time

    {% endif %}

)

select * from data
""".lstrip()



overwrite_static_day_merge_sub_strategy_sql = """
{% set partitions_to_replace = [
  "'2020-01-01'",
  "'2020-01-02'",
] %}

{{
    config(
        materialized="incremental",
        incremental_strategy="insert_overwrite",
        incremental_substrategy='merge',
        cluster_by="id",
        partition_by={
            "field": "date_time",
            "data_type": "datetime",
            "granularity": "day"
        },
        partitions=partitions_to_replace,
        on_schema_change="sync_all_columns"
    )
}}


with data as (

    {% if not is_incremental() %}

        select 1 as id, cast('2020-01-01' as datetime) as date_time union all
        select 2 as id, cast('2020-01-01' as datetime) as date_time union all
        select 3 as id, cast('2020-01-01' as datetime) as date_time union all
        select 4 as id, cast('2020-01-01' as datetime) as date_time

    {% else %}

        -- we want to overwrite the 4 records in the 2020-01-01 partition
        -- with the 2 records below, but add two more in the 2020-01-02 partition
        select 10 as id, cast('2020-01-01' as datetime) as date_time union all
        select 20 as id, cast('2020-01-01' as datetime) as date_time union all
        select 30 as id, cast('2020-01-02' as datetime) as date_time union all
        select 40 as id, cast('2020-01-02' as datetime) as date_time

    {% endif %}

)

select * from data
""".lstrip()


overwrite_static_day_copy_partitions_sub_strategy_sql = """
{% set partitions_to_replace = [
  "'2020-01-01'",
  "'2020-01-02'",
] %}

{{
    config(
        materialized="incremental",
        incremental_strategy="insert_overwrite",
        incremental_substrategy='copy_partitions',
        cluster_by="id",
        partition_by={
            "field": "date_time",
            "data_type": "datetime",
            "granularity": "day"
        },
        partitions=partitions_to_replace,
        on_schema_change="sync_all_columns"
    )
}}


with data as (

    {% if not is_incremental() %}

        select 1 as id, cast('2020-01-01' as datetime) as date_time union all
        select 2 as id, cast('2020-01-01' as datetime) as date_time union all
        select 3 as id, cast('2020-01-01' as datetime) as date_time union all
        select 4 as id, cast('2020-01-01' as datetime) as date_time

    {% else %}

        -- we want to overwrite the 4 records in the 2020-01-01 partition
        -- with the 2 records below, but add two more in the 2020-01-02 partition
        select 10 as id, cast('2020-01-01' as datetime) as date_time union all
        select 20 as id, cast('2020-01-01' as datetime) as date_time union all
        select 30 as id, cast('2020-01-02' as datetime) as date_time union all
        select 40 as id, cast('2020-01-02' as datetime) as date_time

    {% endif %}

)

select * from data
""".lstrip()
