{{ config(
    materialized = 'incremental',
    unique_key = 'state'
)
}}

select * from {{ ref('single_seed') }}