{{
    config(
        materialized='incremental',
        unique_key=['field1', 'field2']
    )
}}

WITH source_data AS (SELECT * FROM {{ ref('model_b') }} )

{% if is_incremental() %}

SELECT id, field1, field2 FROM source_data WHERE id NOT IN (SELECT id from {{ this }} )

{% else %}

SELECT id, field1, field2 FROM source_data LIMIT 2

{% endif %}