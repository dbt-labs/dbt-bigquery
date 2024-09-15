from typing import Dict, Optional, Union
import pytest

from dbt.adapters.bigquery.column import (
    get_nested_column_data_types,
    _update_nested_column_data_types,
    _PARENT_DATA_TYPE_KEY,
)


@pytest.mark.parametrize(
    "column_name, column_data_type, column_rendered_constraint, nested_column_data_types, expected_output",
    [
        # Flat column – with constraints
        ("a", "string", "not null", {}, {"a": "string not null"}),
        # Flat column – with constraints
        ("a", "string", None, {}, {"a": "string"}),
        # Flat column – with constraints
        ("a", None, "not null", {}, {"a": None}),
        # Flat column – without constraints
        ("a", None, None, {}, {"a": None}),
        # Parent nested column – with constraints
        ("a", "struct", "not null", {}, {"a": "struct not null"}),
        # Single nested column, 1 level – with constraints
        ("b.c", "string", "not null", {}, {"b": {"c": "string not null"}}),
        # Single nested column, 1 level – without constraints
        ("b.c", None, None, {}, {"b": {"c": None}}),
        # Second nested column, 1 level – with constraints
        (
            "b.d",
            "int64",
            None,
            {"b": {"c": "string not null"}},
            {"b": {"c": "string not null", "d": "int64"}},
        ),
        # Single nested column, 1 level, parent constraints – with constraints
        (
            "b.c",
            "string",
            None,
            {"b": "struct"},
            {"b": {_PARENT_DATA_TYPE_KEY: "struct", "c": "string"}},
        ),
        # Second nested column, 1 level, parent constraints – with constraints
        (
            "b.d",
            "int64",
            "unique",
            {"b": {_PARENT_DATA_TYPE_KEY: "struct", "c": "string"}},
            {"b": {_PARENT_DATA_TYPE_KEY: "struct", "c": "string", "d": "int64 unique"}},
        ),
        # Combining flat + nested columns
        ("b.c", "string", None, {"a": "string"}, {"a": "string", "b": {"c": "string"}}),
        (
            "a",
            "string",
            "not null",
            {"b": {_PARENT_DATA_TYPE_KEY: "struct", "c": "string", "d": "int64 unique"}},
            {
                "b": {_PARENT_DATA_TYPE_KEY: "struct", "c": "string", "d": "int64 unique"},
                "a": "string not null",
            },
        ),
        # Multiple nested columns
        (
            "c.d",
            None,
            None,
            {"b": {"c": "string not null"}},
            {"b": {"c": "string not null"}, "c": {"d": None}},
        ),
        # Multiple nested columns
        (
            "c.d",
            None,
            None,
            {"b": {"c": "string not null"}},
            {"b": {"c": "string not null"}, "c": {"d": None}},
        ),
        # Multiple levels of nesting
        (
            "b.c.d",
            "string",
            "not null",
            {},
            {"b": {"c": {"d": "string not null"}}},
        ),
        (
            "b.c.e",
            None,
            "not null",
            {"b": {"c": {"d": "string not null"}}},
            {"b": {"c": {"d": "string not null", "e": None}}},
        ),
        (
            "b.c.e",
            None,
            "not null",
            {
                "b": {
                    _PARENT_DATA_TYPE_KEY: "struct",
                    "c": {"d": "string not null"},
                    "d": "int64 unique",
                }
            },
            {
                "b": {
                    _PARENT_DATA_TYPE_KEY: "struct",
                    "c": {"d": "string not null", "e": None},
                    "d": "int64 unique",
                }
            },
        ),
        (
            "b.c.e.f",
            "int64",
            "not null",
            {
                "b": {
                    _PARENT_DATA_TYPE_KEY: "struct",
                    "c": {"d": "string not null"},
                    "d": "int64 unique",
                }
            },
            {
                "b": {
                    _PARENT_DATA_TYPE_KEY: "struct",
                    "c": {"d": "string not null", "e": {"f": "int64 not null"}},
                    "d": "int64 unique",
                }
            },
        ),
    ],
)
def test__update_nested_column_data_types(
    column_name: str,
    column_data_type: Optional[str],
    column_rendered_constraint: Optional[str],
    nested_column_data_types: Dict[str, Optional[Union[str, Dict]]],
    expected_output: Dict[str, Optional[Union[str, Dict]]],
):
    _update_nested_column_data_types(
        column_name, column_data_type, column_rendered_constraint, nested_column_data_types
    )
    assert nested_column_data_types == expected_output


@pytest.mark.parametrize(
    ["columns", "constraints", "expected_nested_columns"],
    [
        ({}, None, {}),
        ({}, {"not_in_columns": "unique"}, {}),
        # Flat column
        (
            {"a": {"name": "a", "data_type": "string"}},
            None,
            {"a": {"name": "a", "data_type": "string"}},
        ),
        # Flat column - missing data_type
        (
            {"a": {"name": "a"}},
            None,
            {"a": {"name": "a", "data_type": None}},
        ),
        # Flat column - with constraints
        (
            {"a": {"name": "a", "data_type": "string"}},
            {"a": "not null"},
            {"a": {"name": "a", "data_type": "string not null"}},
        ),
        # Flat column - with constraints + other keys
        (
            {"a": {"name": "a", "data_type": "string", "quote": True}},
            {"a": "not null"},
            {"a": {"name": "a", "data_type": "string not null", "quote": True}},
        ),
        # Single nested column, 1 level
        (
            {"b.nested": {"name": "b.nested", "data_type": "string"}},
            None,
            {"b": {"name": "b", "data_type": "struct<nested string>"}},
        ),
        # Single nested column, 1 level - missing data_type
        (
            {"b.nested": {"name": "b.nested"}},
            None,
            {"b": {"name": "b", "data_type": "struct<nested>"}},
        ),
        # Single nested column, 1 level - with constraints
        (
            {"b.nested": {"name": "b.nested", "data_type": "string"}},
            {"b.nested": "not null"},
            {"b": {"name": "b", "data_type": "struct<nested string not null>"}},
        ),
        # Single nested column, 1 level - with constraints, missing data_type (constraints not valid without data_type)
        (
            {"b.nested": {"name": "b.nested"}},
            {"b.nested": "not null"},
            {"b": {"name": "b", "data_type": "struct<nested>"}},
        ),
        # Single nested column, 1 level - with constraints + other keys
        (
            {"b.nested": {"name": "b.nested", "data_type": "string", "other": "unpreserved"}},
            {"b.nested": "not null"},
            {"b": {"name": "b", "data_type": "struct<nested string not null>"}},
        ),
        # Single nested column, 1 level - with corresponding parent column
        (
            {
                "b": {"name": "b", "data_type": "struct"},
                "b.nested": {"name": "b.nested", "data_type": "string"},
            },
            None,
            {"b": {"name": "b", "data_type": "struct<nested string>"}},
        ),
        # Single nested column, 1 level - with corresponding parent column specified last
        (
            {
                "b.nested": {"name": "b.nested", "data_type": "string"},
                "b": {"name": "b", "data_type": "struct"},
            },
            None,
            {"b": {"name": "b", "data_type": "struct<nested string>"}},
        ),
        # Single nested column, 1 level - with corresponding parent column + parent constraint
        (
            {
                "b": {"name": "b", "data_type": "struct"},
                "b.nested": {"name": "b.nested", "data_type": "string"},
            },
            {"b": "not null"},
            {"b": {"name": "b", "data_type": "struct<nested string> not null"}},
        ),
        # Single nested column, 1 level - with corresponding parent column as array
        (
            {
                "b": {"name": "b", "data_type": "array"},
                "b.nested": {"name": "b.nested", "data_type": "string"},
            },
            None,
            {"b": {"name": "b", "data_type": "array<struct<nested string>>"}},
        ),
        # Single nested column, 1 level - with corresponding parent column as array + constraint
        (
            {
                "b": {"name": "b", "data_type": "array"},
                "b.nested": {"name": "b.nested", "data_type": "string"},
            },
            {"b": "not null"},
            {"b": {"name": "b", "data_type": "array<struct<nested string>> not null"}},
        ),
        # Multiple nested columns, 1 level
        (
            {
                "b.nested": {"name": "b.nested", "data_type": "string"},
                "b.nested2": {"name": "b.nested2", "data_type": "int64"},
            },
            None,
            {"b": {"name": "b", "data_type": "struct<nested string, nested2 int64>"}},
        ),
        # Multiple nested columns, 1 level - with constraints
        (
            {
                "b.nested": {"name": "b.nested", "data_type": "string"},
                "b.nested2": {"name": "b.nested2", "data_type": "int64"},
            },
            {"b.nested": "not null"},
            {"b": {"name": "b", "data_type": "struct<nested string not null, nested2 int64>"}},
        ),
        # Multiple nested columns, 1 level - with constraints
        (
            {
                "b.nested": {"name": "b.nested", "data_type": "string"},
                "b.nested2": {"name": "b.nested2", "data_type": "int64"},
            },
            {"b.nested": "not null"},
            {"b": {"name": "b", "data_type": "struct<nested string not null, nested2 int64>"}},
        ),
        # Mix of flat and nested columns, 1 level
        (
            {
                "a": {"name": "a", "data_type": "string"},
                "b.nested": {"name": "b.nested", "data_type": "string"},
                "b.nested2": {"name": "b.nested2", "data_type": "int64"},
            },
            None,
            {
                "b": {"name": "b", "data_type": "struct<nested string, nested2 int64>"},
                "a": {"name": "a", "data_type": "string"},
            },
        ),
        # Nested columns, multiple levels
        (
            {
                "b.user.name.first": {
                    "name": "b.user.name.first",
                    "data_type": "string",
                },
                "b.user.name.last": {
                    "name": "b.user.name.last",
                    "data_type": "string",
                },
                "b.user.id": {"name": "b.user.id", "data_type": "int64"},
                "b.user.country": {"name": "b.user.country", "data_type": "string"},
            },
            None,
            {
                "b": {
                    "name": "b",
                    "data_type": "struct<user struct<name struct<first string, last string>, id int64, country string>>",
                },
            },
        ),
        # Nested columns, multiple levels - missing data_type
        (
            {
                "b.user.name.first": {
                    "name": "b.user.name.first",
                    "data_type": "string",
                },
                "b.user.name.last": {
                    "name": "b.user.name.last",
                    "data_type": "string",
                },
                "b.user.id": {"name": "b.user.id", "data_type": "int64"},
                "b.user.country": {"name": "b.user.country"},  # missing data_type
            },
            None,
            {
                "b": {
                    "name": "b",
                    "data_type": "struct<user struct<name struct<first string, last string>, id int64, country>>",
                },
            },
        ),
        # Nested columns, multiple levels - with constraints!
        (
            {
                "b.user.name.first": {
                    "name": "b.user.name.first",
                    "data_type": "string",
                },
                "b.user.name.last": {
                    "name": "b.user.name.last",
                    "data_type": "string",
                },
                "b.user.id": {"name": "b.user.id", "data_type": "int64"},
                "b.user.country": {"name": "b.user.country", "data_type": "string"},
            },
            {"b.user.name.first": "not null", "b.user.id": "unique"},
            {
                "b": {
                    "name": "b",
                    "data_type": "struct<user struct<name struct<first string not null, last string>, id int64 unique, country string>>",
                },
            },
        ),
        # Nested columns, multiple levels - with parent arrays and constraints!
        (
            {
                "b.user.names": {
                    "name": "b.user.names",
                    "data_type": "array",
                },
                "b.user.names.first": {
                    "name": "b.user.names.first",
                    "data_type": "string",
                },
                "b.user.names.last": {
                    "name": "b.user.names.last",
                    "data_type": "string",
                },
                "b.user.id": {"name": "b.user.id", "data_type": "int64"},
                "b.user.country": {"name": "b.user.country", "data_type": "string"},
            },
            {"b.user.names.first": "not null", "b.user.id": "unique"},
            {
                "b": {
                    "name": "b",
                    "data_type": "struct<user struct<names array<struct<first string not null, last string>>, id int64 unique, country string>>",
                },
            },
        ),
    ],
)
def test_get_nested_column_data_types(columns, constraints, expected_nested_columns):
    actual_nested_columns = get_nested_column_data_types(columns, constraints)
    assert expected_nested_columns == actual_nested_columns
