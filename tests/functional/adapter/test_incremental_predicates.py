import pytest
from dbt.tests.adapter.incremental.test_incremental_predicates import BaseIncrementalPredicates


class TestIncrementalPredicatesMergeBigQuery(BaseIncrementalPredicates):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "models": { 
                "+incremental_predicates": [
                    "dbt_internal_dest.id != 2"
                    ],
                "+incremental_strategy": "merge"
            }
        }

class TestPredicatesMergeBigQuery(BaseIncrementalPredicates):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "models": { 
                "+predicates": [
                    "dbt_internal_dest.id != 2"
                    ],
                "+incremental_strategy": "merge"
            }
        }