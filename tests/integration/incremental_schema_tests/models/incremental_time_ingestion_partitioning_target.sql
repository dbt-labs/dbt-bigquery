
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