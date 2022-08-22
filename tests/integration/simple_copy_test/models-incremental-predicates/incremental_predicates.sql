{{
    config(
        materialized = "incremental",
        incremental_strategy="merge",
        unique_key = "id",
        incremental_predicates=[
            "dbt_internal_dest.id > 20"
        ]
    )
}}


select *
from {{ ref('seed') }}

{% if is_incremental() %}

    where load_date > (select max(load_date) from {{this}})

{% endif %}

-- with these incremental predicates, we expect to see duplicate values because we are forcing one of the updated records to be ignored by the merge criteria