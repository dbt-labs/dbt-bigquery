{{
  config(
    materialized='udf',
    persist_docs={ 'relation': true }
  )
}}

-- Dummy UDF that returns the following string
'UDF with description'
