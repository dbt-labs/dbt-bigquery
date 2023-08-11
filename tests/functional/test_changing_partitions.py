import pytest
import json
from dbt.tests.util import run_dbt

_MACRO_SQL = """
{% test number_partitions(model, expected) %}

    {%- set result = get_partitions_metadata(model) %}

    {% if result %}
        {% set partitions = result.columns['partition_id'].values() %}
    {% else %}
        {% set partitions = () %}
    {% endif %}

    {% set actual = partitions | length %}
    {% set success = 1 if model and actual == expected else 0 %}

    select 'Expected {{ expected }}, but got {{ actual }}' as validation_error
    from (select true)
    where {{ success }} = 0

{% endtest %}
"""

_MODEL_SQL = """
{{
    config(
        materialized="table",
        partition_by=var('partition_by'),
        cluster_by=var('cluster_by'),
        partition_expiration_days=var('partition_expiration_days'),
        require_partition_filter=var('require_partition_filter')
    )
}}

select 1 as id, 'dr. bigquery' as name, current_timestamp() as cur_time, current_date() as cur_date
union all
select 2 as id, 'prof. bigquery' as name, current_timestamp() as cur_time, current_date() as cur_date
"""

_SCHEMA_YML = """
version: 2
models:
- name: my_model
  tests:
  - number_partitions:
      expected: "{{ var('expected', 1) }}"
"""


class BaseBigQueryChangingPartition:
    @pytest.fixture(scope="class")
    def macros(self):
        return {"partition_metadata.sql": _MACRO_SQL}

    @pytest.fixture(scope="class")
    def models(self):
        return {"my_model.sql": _MODEL_SQL, "schema.yml": _SCHEMA_YML}

    def run_changes(self, before, after):
        results = run_dbt(["run", "--vars", json.dumps(before)])
        assert len(results) == 1

        results = run_dbt(["run", "--vars", json.dumps(after)])
        assert len(results) == 1

    def partitions_test(self, expected):
        test_results = run_dbt(["test", "--vars", json.dumps(expected)])

        for result in test_results:
            assert result.status == "pass"
            assert not result.skipped
            assert result.failures == 0


class TestBigQueryChangingPartition(BaseBigQueryChangingPartition):
    def test_bigquery_add_partition(self, project):
        before = {
            "partition_by": None,
            "cluster_by": None,
            "partition_expiration_days": None,
            "require_partition_filter": None,
        }
        after = {
            "partition_by": {"field": "cur_time", "data_type": "timestamp"},
            "cluster_by": None,
            "partition_expiration_days": 7,
            "require_partition_filter": True,
        }
        self.run_changes(before, after)
        self.partitions_test({"expected": 1})

    def test_bigquery_add_partition_year(self, project):
        before = {
            "partition_by": None,
            "cluster_by": None,
            "partition_expiration_days": None,
            "require_partition_filter": None,
        }
        after = {
            "partition_by": {"field": "cur_time", "data_type": "timestamp", "granularity": "year"},
            "cluster_by": None,
            "partition_expiration_days": None,
            "require_partition_filter": None,
        }
        self.run_changes(before, after)
        self.partitions_test({"expected": 1})

    def test_bigquery_add_partition_month(self, project):
        before = {
            "partition_by": None,
            "cluster_by": None,
            "partition_expiration_days": None,
            "require_partition_filter": None,
        }
        after = {
            "partition_by": {
                "field": "cur_time",
                "data_type": "timestamp",
                "granularity": "month",
            },
            "cluster_by": None,
            "partition_expiration_days": None,
            "require_partition_filter": None,
        }
        self.run_changes(before, after)
        self.partitions_test({"expected": 1})

    def test_bigquery_add_partition_hour(self, project):
        before = {
            "partition_by": {"field": "cur_time", "data_type": "timestamp", "granularity": "day"},
            "cluster_by": None,
            "partition_expiration_days": None,
            "require_partition_filter": None,
        }
        after = {
            "partition_by": {"field": "cur_time", "data_type": "timestamp", "granularity": "hour"},
            "cluster_by": None,
            "partition_expiration_days": None,
            "require_partition_filter": None,
        }
        self.run_changes(before, after)
        self.partitions_test({"expected": 1})

    def test_bigquery_remove_partition(self, project):
        before = {
            "partition_by": {"field": "cur_time", "data_type": "timestamp"},
            "cluster_by": None,
            "partition_expiration_days": None,
            "require_partition_filter": None,
        }
        after = {
            "partition_by": None,
            "cluster_by": None,
            "partition_expiration_days": None,
            "require_partition_filter": None,
        }
        self.run_changes(before, after)

    def test_bigquery_change_partitions(self, project):
        before = {
            "partition_by": {"field": "cur_time", "data_type": "timestamp"},
            "cluster_by": None,
            "partition_expiration_days": None,
            "require_partition_filter": None,
        }
        after = {
            "partition_by": {"field": "cur_date"},
            "cluster_by": None,
            "partition_expiration_days": 7,
            "require_partition_filter": True,
        }
        self.run_changes(before, after)
        self.partitions_test({"expected": 1})
        self.run_changes(after, before)
        self.partitions_test({"expected": 1})

    def test_bigquery_change_partitions_from_int(self, project):
        before = {
            "partition_by": {
                "field": "id",
                "data_type": "int64",
                "range": {"start": 0, "end": 10, "interval": 1},
            },
            "cluster_by": None,
            "partition_expiration_days": None,
            "require_partition_filter": None,
        }
        after = {
            "partition_by": {"field": "cur_date", "data_type": "date"},
            "cluster_by": None,
            "partition_expiration_days": None,
            "require_partition_filter": None,
        }
        self.run_changes(before, after)
        self.partitions_test({"expected": 1})
        self.run_changes(after, before)
        self.partitions_test({"expected": 2})

    def test_bigquery_add_clustering(self, project):
        before = {
            "partition_by": {"field": "cur_time", "data_type": "timestamp"},
            "cluster_by": None,
            "partition_expiration_days": None,
            "require_partition_filter": None,
        }
        after = {
            "partition_by": {"field": "cur_date"},
            "cluster_by": "id",
            "partition_expiration_days": None,
            "require_partition_filter": None,
        }
        self.run_changes(before, after)

    def test_bigquery_remove_clustering(self, project):
        before = {
            "partition_by": {"field": "cur_time", "data_type": "timestamp"},
            "cluster_by": "id",
            "partition_expiration_days": None,
            "require_partition_filter": None,
        }
        after = {
            "partition_by": {"field": "cur_date"},
            "cluster_by": None,
            "partition_expiration_days": None,
            "require_partition_filter": None,
        }
        self.run_changes(before, after)

    def test_bigquery_change_clustering(self, project):
        before = {
            "partition_by": {"field": "cur_time", "data_type": "timestamp"},
            "cluster_by": "id",
            "partition_expiration_days": None,
            "require_partition_filter": None,
        }
        after = {
            "partition_by": {"field": "cur_date"},
            "cluster_by": "name",
            "partition_expiration_days": None,
            "require_partition_filter": None,
        }
        self.run_changes(before, after)

    def test_bigquery_change_clustering_strict(self, project):
        before = {
            "partition_by": {"field": "cur_time", "data_type": "timestamp"},
            "cluster_by": "id",
            "partition_expiration_days": None,
            "require_partition_filter": None,
        }
        after = {
            "partition_by": {"field": "cur_date", "data_type": "date"},
            "cluster_by": "name",
            "partition_expiration_days": None,
            "require_partition_filter": None,
        }
        self.run_changes(before, after)
