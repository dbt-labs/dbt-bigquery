{{
  config(
    materialized='udf',
    args=[
        {'name': 'color', 'type': 'STRING'},
        {'name': 'is_pretty', 'type': 'BOOL'}
    ]
  )
}}

case
    when is_pretty then concat('pretty', ' ', color)
    else concat('not pretty', ' ', color)
end
