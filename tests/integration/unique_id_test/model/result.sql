{{ config(
  materialized = 'incremental',
  unique_key = ['state', 'city']
)}}

select * from {{ ref('seed') }})