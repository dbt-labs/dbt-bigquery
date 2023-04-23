from dbt.tests.adapter.hooks import test_model_hooks as core_base
import pytest


class TestBigQueryPrePostModelHooks(core_base.TestPrePostModelHooks):
    def check_hooks(self, state, project, host, count=1):
        self.get_ctx_vars(state, count=count, project=project)


class TestBigQueryPrePostModelHooksUnderscores(core_base.TestPrePostModelHooksUnderscores):
    def check_hooks(self, state, project, host, count=1):
        self.get_ctx_vars(state, count=count, project=project)


class TestBigQueryHookRefs(core_base.TestHookRefs):
    def check_hooks(self, state, project, host, count=1):
        self.get_ctx_vars(state, count=count, project=project)


class TestBigQueryPrePostModelHooksOnSeeds(core_base.TestPrePostModelHooksOnSeeds):
    def check_hooks(self, state, project, host, count=1):
        self.get_ctx_vars(state, count=count, project=project)

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
