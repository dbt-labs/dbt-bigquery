import pytest
import re
import json
from dbt.tests.util import (
    run_dbt,
    get_manifest,
    run_dbt_and_capture
)
from dbt.tests.adapter.constraints.test_constraints import (
    TestModelLevelConstraintsEnabledConfigs,
    TestModelLevelConstraintsDisabledConfigs,
    TestSchemaConstraintsEnabledConfigs,
    TestModelLevelConstraintsErrorMessages
)


my_model_sql = """
{{
  config(
    materialized = "table"
  )
}}
select
  1 as id,
  'blue' as color,
  cast('2019-01-01' as date) as date_day
"""

my_model_error_sql = """
{{
  config(
    materialized = "table"
  )
}}
select
  null as id,
  'blue' as color,
  cast('2019-01-01' as date) as date_day
"""

model_schema_yml = """
version: 2
models:
  - name: my_model
    config:
      constraints_enabled: true
    columns:
      - name: id
        data_type: integer
        description: hello
        constraints: ['not null']
        check: (id > 0)
        tests:
          - unique
      - name: color
        data_type: string
      - name: date_day
        data_type: date
  - name: my_model_error
    config:
      constraints_enabled: true
    columns:
      - name: id
        data_type: integer
        description: hello
        constraints: ['not null']
        check: (id > 0)
        tests:
          - unique
      - name: color
        data_type: string
      - name: date_day
        data_type: date
"""

database_name = "database_placeholder"
schema_name = "schema_placeholder"

_expected_sql = f"""
  create or replace table `{database_name}`.`{schema_name}`.`my_model`

    
  
  (
    
      id integer  ,
      color string  ,
      date_day date  
  )
  

    
    
    
    OPTIONS()
    as (
      
select
  1 as id,
  'blue' as color,
  cast('2019-01-01' as date) as date_day
    );
"""


class BaseConstraintsEnabledModelvsProject:
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "models": {
                "test": {
                    "+constraints_enabled": True,
                    "subdirectory": {
                        "+constraints_enabled": False,
                    },
                }
            }
        }


class TestBigQueryConstraints(BaseConstraintsEnabledModelvsProject):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "my_model.sql": my_model_sql,
            "my_model_error.sql": my_model_error_sql,
            "constraints_schema.yml": model_schema_yml,
        }

    @pytest.fixture(scope="class")
    def expected_sql(self):
        return _expected_sql

    def test__model_constraints_DDL(self, project, expected_sql):
        results = run_dbt(["run", "-s", "my_model"])
        assert len(results) == 1
        with open("./target/run/test/models/my_model.sql", "r") as fp:
            generated_sql = fp.read()

        with open("./target/manifest.json", "r") as fp:
            generated_manifest = json.load(fp)

        model_unique_id = 'model.test.my_model'
        schema_name_generated = (generated_manifest['nodes'][model_unique_id]['schema'])
        database_name_generated = (generated_manifest['nodes'][model_unique_id]['database'])

        if expected_sql:
            expected_sql = expected_sql.replace(schema_name, schema_name_generated)
            expected_sql = expected_sql.replace(database_name, database_name_generated)
            generated_sql_check = re.sub(r"\s+", "", generated_sql).lower()
            expected_sql_check = re.sub(r"\s+", "", expected_sql).lower()
            assert (
                expected_sql_check == generated_sql_check
            ), f"generated sql did not match expected: {generated_sql}"

    def test__rollback(self, project):
        results = run_dbt(["run", "-s", "my_model"])
        assert len(results) == 1

        with open("./models/my_model.sql", "r") as fp:
            my_model_sql_original = fp.read()

        my_model_sql_error = my_model_sql_original.replace("1 as id", "null as id")

        with open("./models/my_model.sql", "w") as fp:
            fp.write(my_model_sql_error)

        results = run_dbt(["run", "-s", "my_model"], expect_pass=False)
        assert len(results) == 1

        with open("./target/manifest.json", "r") as fp:
            generated_manifest = json.load(fp)

        model_unique_id = 'model.test.my_model'
        schema_name_generated = (generated_manifest['nodes'][model_unique_id]['schema'])
        database_name_generated = (generated_manifest['nodes'][model_unique_id]['database'])

        # verify the previous table exists
        sql = f"""
            select id from {database_name_generated}.{schema_name_generated}.my_model where id = 1
        """
        results = project.run_sql(sql, fetch="all")
        assert len(results) == 1
        assert results[0][0] == 1

    def test__constraints_enforcement(self, project):

        results, log_output = run_dbt_and_capture(['run', '-s', 'my_model_error'], expect_pass=False)
        manifest = get_manifest(project.project_root)
        model_id = "model.test.my_model_error"
        my_model_config = manifest.nodes[model_id].config
        constraints_enabled_actual_config = my_model_config.constraints_enabled

        assert constraints_enabled_actual_config is True

        expected_constraints_error = 'Cannot insert a NULL value into column id'
        assert expected_constraints_error in log_output