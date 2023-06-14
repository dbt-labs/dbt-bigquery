import pytest

from dbt.adapters.bigquery.column import get_nested_column_data_types


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
        # Single nested column, 1 level - with constraints
        (
            {"b.nested": {"name": "b.nested", "data_type": "string"}},
            {"b.nested": "not null"},
            {"b": {"name": "b", "data_type": "struct<nested string not null>"}},
        ),
        # Single nested column, 1 level - with constraints + other keys
        (
            {"b.nested": {"name": "b.nested", "data_type": "string", "other": "unpreserved"}},
            {"b.nested": "not null"},
            {"b": {"name": "b", "data_type": "struct<nested string not null>"}},
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
    ],
)
def test_get_nested_column_data_types(columns, constraints, expected_nested_columns):
    actual_nested_columns = get_nested_column_data_types(columns, constraints)
    assert expected_nested_columns == actual_nested_columns
