import pytest
from dbt.tests.adapter.unit_testing.test_types import BaseUnitTestingTypes
from dbt.tests.adapter.unit_testing.test_case_insensitivity import BaseUnitTestCaseInsensivity
from dbt.tests.adapter.unit_testing.test_invalid_input import BaseUnitTestInvalidInput


class TestBigQueryUnitTestingTypes(BaseUnitTestingTypes):
    @pytest.fixture
    def data_types(self):
        # sql_value, yaml_value
        return [
            ["1", "1"],
            ["'1'", "1"],
            ["cast('true' as boolean)", "true"],
            ["1.0", "1.0"],
            ["'string value'", "string value"],
            ["cast(1.0 as numeric)", "1.0"],
            ["cast(1 as bigint)", 1],
            ["cast('2019-01-01' as date)", "2019-01-01"],
            ["cast('2013-11-03 00:00:00-07' as timestamp)", "2013-11-03 00:00:00-07"],
            ["st_geogpoint(75, 45)", "'st_geogpoint(75, 45)'"],
            # arrays
            ["cast(['a','b','c'] as array<string>)", "['a','b','c']"],
            ["cast([1,2,3] as array<int>)", "[1,2,3]"],
            ["cast([true,true,false] as array<bool>)", "[true,true,false]"],
            # array of date
            ["[date '2019-01-01']", "['2020-01-01']"],
            ["[date '2019-01-01']", "[]"],
            ["[date '2019-01-01']", "null"],
            # array of timestamp
            ["[timestamp '2019-01-01']", "['2020-01-01']"],
            ["[timestamp '2019-01-01']", "[]"],
            ["[timestamp '2019-01-01']", "null"],
            # json
            [
                """json '{"name": "Cooper", "forname": "Alice"}'""",
                """{"name": "Cooper", "forname": "Alice"}""",
            ],
            ["""json '{"name": "Cooper", "forname": "Alice"}'""", "{}"],
            # structs
            ["struct('Isha' as name, 22 as age)", """'struct("Isha" as name, 22 as age)'"""],
            [
                "struct('Kipketer' AS name, [23.2, 26.1, 27.3, 29.4] AS laps)",
                """'struct("Kipketer" AS name, [23.2, 26.1, 27.3, 29.4] AS laps)'""",
            ],
            # struct of struct
            [
                "struct(struct(1 as id, 'blue' as color) as my_struct)",
                """'struct(struct(1 as id, "blue" as color) as my_struct)'""",
            ],
            # array of struct
            [
                "[struct(st_geogpoint(75, 45) as my_point), struct(st_geogpoint(75, 35) as my_point)]",
                "['struct(st_geogpoint(75, 45) as my_point)', 'struct(st_geogpoint(75, 35) as my_point)']",
            ],
        ]


class TestBigQueryUnitTestCaseInsensitivity(BaseUnitTestCaseInsensivity):
    pass


class TestBigqueryeUnitTestInvalidInput(BaseUnitTestInvalidInput):
    pass
