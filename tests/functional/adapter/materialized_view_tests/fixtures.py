import pytest
from dbt.tests.adapter.materialized_view.base import Base


class BigQueryBasicBase(Base):
    @pytest.fixture(scope="class")
    def models(self):
        base_table = """
        {{ config(materialized='table') }}
        select 1 as base_column
        """
        base_materialized_view = """
        {{ config(materialized='materialized_view') }}
        select * from {{ ref('base_table') }}
        """
        return {"base_table.sql": base_table, "base_materialized_view.sql": base_materialized_view}
