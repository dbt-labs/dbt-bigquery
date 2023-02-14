from dbt.tests.adapter.hooks import test_model_hooks as core_base
import pytest


class TestBigQueryPrePostModelHooks(core_base.TestPrePostModelHooks):
    pass


class TestBigQueryPrePostModelHooksUnderscores(core_base.TestPrePostModelHooksUnderscores):
    pass


class TestBigQueryHookRefs(core_base.TestHookRefs):
    pass


class TestBigQueryPrePostModelHooksOnSeeds(core_base.TestPrePostModelHooksOnSeeds):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "seed-paths": ["seeds"],
            "models": {},
            "seeds": {
                "+post-hook": [
                    "alter table {{ this }} add column new_col int",
                    "update {{ this }} set new_col = 1 where 1=1",
                ],
                "quote_columns": True,
            },
        }
