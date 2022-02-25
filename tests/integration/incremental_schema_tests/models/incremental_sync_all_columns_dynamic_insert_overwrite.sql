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
