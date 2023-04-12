SNAPSHOT_TIMESTAMP_SQL = """
{% snapshot snapshot %}
    {{ config(
        target_database=database,
        target_schema=schema,
        unique_key='id',
        strategy='timestamp',
        updated_at='updated_at_ts',
        invalidate_hard_deletes=True,
    ) }}
    select *, timestamp(updated_at) as updated_at_ts from {{ ref('fact') }}
{% endsnapshot %}
"""


SNAPSHOT_CHECK_SQL = """
{% snapshot snapshot %}
    {{ config(
        target_database=database,
        target_schema=schema,
        unique_key='id',
        strategy='check',
        check_cols=['email'],
    ) }}
    select * from {{ ref('fact') }}
{% endsnapshot %}
"""
