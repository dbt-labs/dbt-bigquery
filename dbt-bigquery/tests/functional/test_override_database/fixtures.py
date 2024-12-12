import pytest
from dbt.tests.fixtures.project import write_project_files


models__view_2_sql = """
{%- if target.type == 'bigquery' -%}
  {{ config(project=var('alternate_db')) }}
{%- else -%}
  {{ config(database=var('alternate_db')) }}
{%- endif -%}
select * from {{ ref('seed') }}

"""

models__view_1_sql = """
{#
    We are running against a database that must be quoted.
    These calls ensure that we trigger an error if we're failing to quote at parse-time
#}
{% do adapter.already_exists(this.schema, this.table) %}
{% do adapter.get_relation(this.database, this.schema, this.table) %}
select * from {{ ref('seed') }}

"""

models__subfolder__view_4_sql = """
{{
    config(database=var('alternate_db'))
}}

select * from {{ ref('seed') }}

"""

models__subfolder__view_3_sql = """
select * from {{ ref('seed') }}

"""

seeds__seed_csv = """id,name
1,a
2,b
3,c
4,d
5,e
"""


@pytest.fixture(scope="class")
def models():
    return {
        "view_2.sql": models__view_2_sql,
        "view_1.sql": models__view_1_sql,
        "subfolder": {
            "view_4.sql": models__subfolder__view_4_sql,
            "view_3.sql": models__subfolder__view_3_sql,
        },
    }


@pytest.fixture(scope="class")
def seeds():
    return {"seed.csv": seeds__seed_csv}


@pytest.fixture(scope="class")
def project_files(
    project_root,
    models,
    seeds,
):
    write_project_files(project_root, "models", models)
    write_project_files(project_root, "seeds", seeds)
