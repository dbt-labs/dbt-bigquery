{{
  config(
    materialized='udf',
    return_type='INT64',
    persist_docs={ 'relation': false }
  )
}}

ifnull(null,0,1)
