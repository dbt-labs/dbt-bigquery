import pytest

from dbt.tests.adapter.simple_seed.test_seed import SeedConfigBase

from dbt.tests.adapter.incremental.fixtures import (
    _MODELS__A,
)

from dbt.tests.util import (
    check_relations_equal,
    run_dbt,
)


class TestIncrementalPartitionInformation:
    pass


def incremental_model_int64_partition_information(partition_information: str):
    return """
            {{
                config(
                    materialized='incremental',
                    unique_key='id',
                    partition_by={
                        "field": "id",
                        "data_type": "int64",
                        "partition_information": "{partition_information}",
                        "range": {
                            "start": 1,
                            "end": 7,
                            "interval": 1
                        },
                        "copy_partitions": true
                    },
                    incremental_strategy='insert_overwrite'
                )
            }}

            WITH source_data AS (SELECT * FROM {{ ref('model_a') }} )

            {% set string_type = 'string' %}

            {% if is_incremental() %}

            SELECT id,
                   cast(field1 as {{string_type}}) as field1,

            FROM source_data WHERE id > _dbt_max_partition

            {% else %}

            SELECT id,
                   cast(field1 as {{string_type}}) as field1,
                   cast(field2 as {{string_type}}) as field2

            FROM source_data WHERE id <= 3

            {% endif %}
            """.replace(
        "{partition_information}", partition_information
    )


def incremental_model_time_ingestion_partition_information(partition_information: str):
    return """
            {{
                config(
                    materialized='incremental',
                    unique_key='id',
                    partition_by={
                        "field": "date_hour",
                        "data_type": "datetime",
                        "partition_information": "{partition_information}",
                        "granularity": "hour",
                        "time_ingestion_partitioning": true
                    },
                    incremental_strategy='insert_overwrite'
                )
            }}

           with data as (

                {% if not is_incremental() %}

                    select 1 as id,
                    cast('2020-01-01 01:00:00' as datetime) as date_hour,
                    2 as field_2 union all
                    select 2 as id,
                    cast('2020-01-01 01:00:00' as datetime) as date_hour,
                    2 as field_2 union all
                    select 3 as id,
                    cast('2020-01-01 01:00:00' as datetime) as date_hour,
                    2 as field_2 union all
                    select 4 as id,
                    cast('2020-01-01 01:00:00' as datetime) as date_hour,
                    2 as field_2

                {% else %}

                    -- we want to overwrite the 4 records in the 2020-01-01 01:00:00 partition
                    -- with the 2 records below, but add two more in the 2020-01-00 02:00:00 partition
                    select 10 as id,
                    cast('2020-01-01 01:00:00' as datetime) as date_hour,
                    2 as field_2 union all
                    select 20 as id,
                    cast('2020-01-01 01:00:00' as datetime) as date_hour,
                    2 as field_2 union all
                    select 30 as id,
                    cast('2020-01-01 02:00:00' as datetime) as date_hour,
                    2 as field_2 union all
                    select 40 as id,
                    cast('2020-01-01 02:00:00' as datetime) as date_hour,
                    2 as field_2

                {% endif %}

            )

            select * from data
            """.replace(
        "{partition_information}", partition_information
    )


def incremental_model_date_partition_information(partition_information: str):
    return """
            {
                config(
                    materialized='incremental',
                    unique_key='id',
                    partition_by={
                        "field": "date",
                        "data_type": "date",
                        "partition_information": "{partition_information}",
                        "granularity": "day",
                    },
                    incremental_strategy='insert_overwrite'
                )
            }

           with data as (

                {% if not is_incremental() %}

                    select 1 as id,
                    cast('2020-01-01' as date) as date,
                    1 as field_1,
                    2 as field_2 union all
                    select 2 as id,
                    cast('2020-01-01' as date) as date,
                    1 as field_1,
                    2 as field_2 union all
                    select 3 as id,
                    cast('2020-01-01' as date) as date,
                    1 as field_1,
                    2 as field_2 union all
                    select 4 as id,
                    cast('2020-01-01' as date) as date,
                    1 as field_1,
                    2 as field_2

                {% else %}

                    -- we want to overwrite the 4 records in the 2020-01-01 01:00:00 partition
                    -- with the 2 records below, but add two more in the 2020-01-00 02:00:00 partition
                    select 10 as id,
                    cast('2020-01-01' as date) as date,
                    2 as field_2 union all
                    select 20 as id,
                    cast('2020-01-01' as date) as date,
                    2 as field_2 union all
                    select 30 as id,
                    cast('2020-01-02' as date) as date,
                    2 as field_2 union all
                    select 40 as id,
                    cast('2020-01-02' as date) as date,
                    2 as field_2

                {% endif %}

            )

            select * from data
            """.replace(
        "{partition_information}", partition_information
    )


class TestIncrementalPartitionInformationBigQuerySpecific(SeedConfigBase):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "model_a.sql": _MODELS__A,
            "incremental_model_int64_partition_information_information_schema.sql": incremental_model_int64_partition_information(
                "information_schema"
            ),
            "incremental_model_int64_partition_information_model.sql": incremental_model_int64_partition_information(
                "model"
            ),
            "incremental_model_time_ingestion_partition_information_information_schema.sql": incremental_model_time_ingestion_partition_information(
                "information_schema"
            ),
            "incremental_model_time_ingestion_partition_information_model.sql": incremental_model_time_ingestion_partition_information(
                "model"
            ),
        }

    def run_twice_and_assert(
        self, include, compare_source, compare_target, project, expected_model_count=3
    ):
        # dbt run (twice)
        run_args = ["run"]
        if include:
            run_args.extend(("--select", include))
        results_one = run_dbt(run_args)
        assert len(results_one) == expected_model_count

        results_two = run_dbt(run_args)
        assert len(results_two) == expected_model_count

        check_relations_equal(project.adapter, [compare_source, compare_target])

    def test_run_incremental_model_int64_partition_information(self, project):
        select = (
            "model_a incremental_model_int64_partition_information_information_schema "
            "incremental_model_int64_partition_information_model"
        )
        compare_source = "incremental_model_int64_partition_information_information_schema"
        compare_target = "incremental_model_int64_partition_information_model"
        self.run_twice_and_assert(select, compare_source, compare_target, project)

    def test_run_incremental_model_time_ingestion_partition_information(self, project):
        select = (
            "incremental_model_time_ingestion_partition_information_information_schema "
            "incremental_model_time_ingestion_partition_information_model"
        )
        compare_source = (
            "incremental_model_time_ingestion_partition_information_information_schema"
        )
        compare_target = "incremental_model_time_ingestion_partition_information_model"
        self.run_twice_and_assert(
            select, compare_source, compare_target, project, expected_model_count=2
        )
