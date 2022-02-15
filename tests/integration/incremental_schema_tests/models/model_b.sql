{{ 
    config(materialized='table') 
}}

with source_data as (

    select 1 as id, 'Texas' as field1, 'Austin' as field2
    union all select 2 as id, 'California' as field1, 'San Diego' as field2
    union all select 3 as id, 'Texas' as field1, 'Dallas' as field2

)

select id
       ,field1
       ,field2
    
from source_data