import random

import pytest
from google.api_core.exceptions import NotFound

from dbt.tests.adapter.utils.test_array_append import BaseArrayAppend
from dbt.tests.adapter.utils.test_array_concat import BaseArrayConcat
from dbt.tests.adapter.utils.test_array_construct import BaseArrayConstruct
from dbt.tests.adapter.utils.test_any_value import BaseAnyValue
from dbt.tests.adapter.utils.test_bool_or import BaseBoolOr
from dbt.tests.adapter.utils.test_cast_bool_to_text import BaseCastBoolToText
from dbt.tests.adapter.utils.test_concat import BaseConcat
from dbt.tests.adapter.utils.test_current_timestamp import BaseCurrentTimestampAware
from dbt.tests.adapter.utils.test_dateadd import BaseDateAdd
from dbt.tests.adapter.utils.test_datediff import BaseDateDiff
from dbt.tests.adapter.utils.test_date_trunc import BaseDateTrunc
from dbt.tests.adapter.utils.test_escape_single_quotes import BaseEscapeSingleQuotesBackslash
from dbt.tests.adapter.utils.test_except import BaseExcept
from dbt.tests.adapter.utils.test_hash import BaseHash
from dbt.tests.adapter.utils.test_intersect import BaseIntersect
from dbt.tests.adapter.utils.test_last_day import BaseLastDay
from dbt.tests.adapter.utils.test_length import BaseLength
from dbt.tests.adapter.utils.test_listagg import BaseListagg
from dbt.tests.adapter.utils.test_position import BasePosition
from dbt.tests.adapter.utils.test_replace import BaseReplace
from dbt.tests.adapter.utils.test_right import BaseRight
from dbt.tests.adapter.utils.test_safe_cast import BaseSafeCast
from dbt.tests.adapter.utils.test_split_part import BaseSplitPart
from dbt.tests.adapter.utils.test_string_literal import BaseStringLiteral
from dbt.tests.adapter.utils.test_validate_sql import BaseValidateSqlMethod
from tests.functional.adapter.utils.fixture_array_append import (
    models__array_append_actual_sql,
    models__array_append_expected_sql,
)
from tests.functional.adapter.utils.fixture_array_concat import (
    models__array_concat_actual_sql,
    models__array_concat_expected_sql,
)
from tests.functional.adapter.utils.fixture_array_construct import (
    models__array_construct_actual_sql,
    models__array_construct_expected_sql,
    macros__array_to_string_sql,
)


class TestAnyValue(BaseAnyValue):
    pass


class TestArrayAppend(BaseArrayAppend):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "actual.sql": models__array_append_actual_sql,
            "expected.sql": models__array_append_expected_sql,
        }

    @pytest.fixture(scope="class")
    def macros(self):
        return {
            "array_to_string.sql": macros__array_to_string_sql,
        }


class TestArrayConcat(BaseArrayConcat):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "actual.sql": models__array_concat_actual_sql,
            "expected.sql": models__array_concat_expected_sql,
        }

    @pytest.fixture(scope="class")
    def macros(self):
        return {
            "array_to_string.sql": macros__array_to_string_sql,
        }


class TestArrayConstruct(BaseArrayConstruct):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "actual.sql": models__array_construct_actual_sql,
            "expected.sql": models__array_construct_expected_sql,
        }

    @pytest.fixture(scope="class")
    def macros(self):
        return {
            "array_to_string.sql": macros__array_to_string_sql,
        }


class TestBoolOr(BaseBoolOr):
    pass


class TestCastBoolToText(BaseCastBoolToText):
    pass


class TestConcat(BaseConcat):
    pass


# Use either BaseCurrentTimestampAware or BaseCurrentTimestampNaive but not both
class TestCurrentTimestamp(BaseCurrentTimestampAware):
    pass


class TestDateAdd(BaseDateAdd):
    pass


class TestDateDiff(BaseDateDiff):
    pass


class TestDateTrunc(BaseDateTrunc):
    pass


class TestEscapeSingleQuotes(BaseEscapeSingleQuotesBackslash):
    pass


class TestExcept(BaseExcept):
    pass


class TestHash(BaseHash):
    pass


class TestIntersect(BaseIntersect):
    pass


class TestLastDay(BaseLastDay):
    pass


class TestLength(BaseLength):
    pass


class TestListagg(BaseListagg):
    pass


class TestPosition(BasePosition):
    pass


class TestReplace(BaseReplace):
    pass


class TestRight(BaseRight):
    pass


class TestSafeCast(BaseSafeCast):
    pass


class TestSplitPart(BaseSplitPart):
    pass


class TestStringLiteral(BaseStringLiteral):
    pass


class TestValidateSqlMethod(BaseValidateSqlMethod):
    pass


class TestDryRunMethod:
    """Test connection manager dry run method operation."""

    def test_dry_run_method(self, project) -> None:
        """Test dry run method on a DDL statement.

        This allows us to demonstrate that no SQL is executed.
        """
        with project.adapter.connection_named("_test"):
            client = project.adapter.connections.get_thread_connection().handle
            random_suffix = "".join(random.choices([str(i) for i in range(10)], k=10))
            table_name = f"test_dry_run_{random_suffix}"
            table_id = "{}.{}.{}".format(project.database, project.test_schema, table_name)
            res = project.adapter.connections.dry_run(f"CREATE TABLE {table_id} (x INT64)")
            assert res.code == "DRY RUN"
            with pytest.raises(expected_exception=NotFound):
                client.get_table(table_id)
