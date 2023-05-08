import pytest
from dbt.tests.util import (
    run_dbt,
    get_connection,
    relation_from_name,
    write_config_file,
)

from dbt.adapters.bigquery import BigQueryRelation

_POLICY_TAG_MODEL = """{{
  config(
    materialized='table',
    persist_docs={ 'columns': true }
  )
}}

select
  struct(
    1 as field
  ) as first_struct
"""

_POLICY_TAG_YML = """version: 2

models:
- name: policy_tag_table
  columns:
  - name: first_struct
  - name: first_struct.field
    policy_tags:
      - '{{ var("policy_tag") }}'
"""

_POLICY_TAG_YML_NO_POLICY_TAGS = """version: 2

models:
- name: policy_tag_table
  columns:
  - name: first_struct
  - name: first_struct.field
"""

# Manually generated https://console.cloud.google.com/bigquery/policy-tags?project=dbt-test-env
_POLICY_TAG = "projects/dbt-test-env/locations/us/taxonomies/5785568062805976401/policyTags/135489647357012267"
_POLICY_TAG_MODEL_NAME = "policy_tag_table"


class TestBigqueryDeleteColumnPolicy:
    """See BQ docs for more info on policy tags:
    https://cloud.google.com/bigquery/docs/column-level-security#work_with_policy_tags
    """

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"config-version": 2, "vars": {"policy_tag": _POLICY_TAG}}

    @pytest.fixture(scope="class")
    def models(self):
        return {
            f"{_POLICY_TAG_MODEL_NAME}.sql": _POLICY_TAG_MODEL,
            "schema.yml": _POLICY_TAG_YML,
        }

    def test_bigquery_delete_column_policy_tag(self, project):
        results = run_dbt(["run", "-f", "--models", "policy_tag_table"])
        assert len(results) == 1
        write_config_file(
            _POLICY_TAG_YML_NO_POLICY_TAGS,
            project.project_root + "/models",
            "schema.yml",
        )  # update the model to remove the policy tag
        new_results = run_dbt(["run", "-f", "--models", "policy_tag_table"])
        assert len(new_results) == 1
        relation: BigQueryRelation = relation_from_name(
            project.adapter, _POLICY_TAG_MODEL_NAME
        )
        adapter = project.adapter
        with get_connection(project.adapter) as conn:
            table = conn.handle.get_table(
                adapter.connections.get_bq_table(
                    relation.database, relation.schema, relation.table
                )
            )
            for schema_field in table.schema:
                assert schema_field.policy_tags is None
