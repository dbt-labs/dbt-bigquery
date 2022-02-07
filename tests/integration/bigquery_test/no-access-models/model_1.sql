{{ config(project = env_var('DBT_ENV_SECRET_BIGQUERY_TEST_NO_ACCESS_DATABASE')) }}

select 1 as id
