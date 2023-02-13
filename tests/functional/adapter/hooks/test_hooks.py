from dbt.tests.adapter.hooks.test_model_hooks import TestPrePostModelHooksUnderscores, TestPrePostModelHooks, \
    TestHookRefs, TestPrePostModelHooksOnSeeds, TestPrePostModelHooksOnSeedsPlusPrefixedWhitespace, \
    TestPrePostModelHooksOnSnapshots, PrePostModelHooksInConfigSetup


class BigQueryTestPrePostModelHooks(TestPrePostModelHooks):
    pass


class BigQueryTestPrePostModelHooksUnderscores(TestPrePostModelHooksUnderscores):
    pass

class BigQueryTestHookRefs(TestHookRefs):
    pass

class BigQueryTestPrePostModelHooksOnSeeds(TestPrePostModelHooksOnSeeds):
    pass

class BigQueryTestPrePostModelHooksOnSeedsPlusPrefixedWhitespace(TestPrePostModelHooksOnSeedsPlusPrefixedWhitespace):
    pass

class BigQueryTestPrePostModelHooksOnSnapshots(TestPrePostModelHooksOnSnapshots):
    pass

class BigQueryPrePostModelHooksInConfigSetup(PrePostModelHooksInConfigSetup):
    pass