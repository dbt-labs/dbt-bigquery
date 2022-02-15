{{ 
    config(materialized='table') 
}}

with source_data as (

    select * from {{ ref('model_b') }}

)

select id
       ,field1
       ,field2

from source_data