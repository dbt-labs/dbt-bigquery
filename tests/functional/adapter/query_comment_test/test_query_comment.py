from dbt.tests.adapter.query_comment.test_query_comment import (
    BaseQueryComments,
    BaseMacroQueryComments,
    BaseMacroArgsQueryComments,
    BaseMacroInvalidQueryComments,
    BaseNullQueryComments,
    BaseEmptyQueryComments,
)


class TestQueryCommentsBigQuery(BaseQueryComments):
    pass


class TestMacroQueryCommentsBigQuery(BaseMacroQueryComments):
    pass


class TestMacroArgsQueryCommentsBigQuery(BaseMacroArgsQueryComments):
    pass


class TestMacroInvalidQueryCommentsBigQuery(BaseMacroInvalidQueryComments):
    pass


class TestNullQueryCommentsBigQuery(BaseNullQueryComments):
    pass


class TestEmptyQueryCommentsBigQuery(BaseEmptyQueryComments):
    pass
