from datetime import datetime

import pytest
from dbt.contracts.relation import RelationType
from dbt.tests.adapter.materialized_view.auto_refresh import (
    MaterializedViewAutoRefreshNoChanges,
)

from dbt.adapters.bigquery.relation import BigQueryRelation

from tests.functional.adapter.materialized_view_tests import files


class TestMaterializedViewAutoRefreshNoChanges(MaterializedViewAutoRefreshNoChanges):
    @pytest.fixture(scope="class", autouse=True)
    def seeds(self):
        yield {"my_seed.csv": files.MY_SEED}

    @pytest.fixture(scope="class", autouse=True)
    def models(self):
        yield {
            "auto_refresh_on.sql": files.MY_MATERIALIZED_VIEW_AUTOREFRESH_ON,
            "auto_refresh_off.sql": files.MY_MATERIALIZED_VIEW_AUTOREFRESH_OFF,
        }

    def last_refreshed(self, project, materialized_view: str) -> datetime:
        relation = BigQueryRelation.create(
            database=project.database,
            schema=project.test_schema,
            identifier=materialized_view,
            type=RelationType.MaterializedView,
        )
        with project.adapter.connection_named("__test"):
            last_refresh_results = project.adapter.get_bq_table(relation)
        return last_refresh_results.mview_last_refresh_time or last_refresh_results.created
