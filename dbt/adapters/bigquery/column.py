from dataclasses import dataclass
from typing import Optional, List, TypeVar, Iterable, Type, Any, Dict, Union

from dbt.adapters.base.column import Column

from google.cloud.bigquery import SchemaField

Self = TypeVar("Self", bound="BigQueryColumn")


@dataclass(init=False)
class BigQueryColumn(Column):
    TYPE_LABELS = {
        "STRING": "STRING",
        "TIMESTAMP": "TIMESTAMP",
        "FLOAT": "FLOAT64",
        "INTEGER": "INT64",
        "BOOLEAN": "BOOLEAN",
        "RECORD": "RECORD",
    }
    fields: List[Self]  # type: ignore
    mode: str  # type: ignore

    def __init__(
        self,
        column: str,
        dtype: str,
        fields: Optional[Iterable[SchemaField]] = None,
        mode: str = "NULLABLE",
    ) -> None:
        super().__init__(column, dtype)

        if fields is None:
            fields = []

        self.fields = self.wrap_subfields(fields)
        self.mode = mode

    @classmethod
    def wrap_subfields(cls: Type[Self], fields: Iterable[SchemaField]) -> List[Self]:
        return [cls.create_from_field(field) for field in fields]

    @classmethod
    def create_from_field(cls: Type[Self], field: SchemaField) -> Self:
        return cls(
            field.name,
            cls.translate_type(field.field_type),
            field.fields,
            field.mode,
        )

    @classmethod
    def _flatten_recursive(cls: Type[Self], col: Self, prefix: Optional[str] = None) -> List[Self]:
        if prefix is None:
            prefix = []  # type: ignore[assignment]

        if len(col.fields) == 0:
            prefixed_name = ".".join(prefix + [col.column])  # type: ignore[operator]
            new_col = cls(prefixed_name, col.dtype, col.fields, col.mode)
            return [new_col]

        new_fields = []
        for field in col.fields:
            new_prefix = prefix + [col.column]  # type: ignore[operator]
            new_fields.extend(cls._flatten_recursive(field, new_prefix))

        return new_fields

    def flatten(self):
        return self._flatten_recursive(self)

    @property
    def quoted(self):
        return "`{}`".format(self.column)

    def literal(self, value):
        return "cast({} as {})".format(value, self.dtype)

    @property
    def data_type(self) -> str:
        if self.dtype.upper() == "RECORD":
            subcols = [
                "{} {}".format(col.name, col.data_type) for col in self.fields  # type: ignore[attr-defined]
            ]
            field_type = "STRUCT<{}>".format(", ".join(subcols))

        else:
            field_type = self.dtype

        if self.mode.upper() == "REPEATED":
            return "ARRAY<{}>".format(field_type)

        else:
            return field_type

    @classmethod
    def numeric_type(cls, dtype: str, precision: Any, scale: Any) -> str:
        # BigQuery makes life much harder if precision + scale are specified
        # even if they're fed in here, just return the data type by itself
        return dtype

    def is_string(self) -> bool:
        return self.dtype.lower() == "string"

    def is_integer(self) -> bool:
        return self.dtype.lower() == "int64"

    def is_numeric(self) -> bool:
        return self.dtype.lower() == "numeric"

    def is_float(self):
        return self.dtype.lower() == "float64"

    def can_expand_to(self: Self, other_column: Self) -> bool:  # type: ignore
        """returns True if both columns are strings"""
        return self.is_string() and other_column.is_string()

    def __repr__(self) -> str:
        return "<BigQueryColumn {} ({}, {})>".format(self.name, self.data_type, self.mode)

    def column_to_bq_schema(self) -> SchemaField:
        """Convert a column to a bigquery schema object."""
        kwargs = {}
        if len(self.fields) > 0:
            fields = [field.column_to_bq_schema() for field in self.fields]  # type: ignore[attr-defined]
            kwargs = {"fields": fields}

        return SchemaField(self.name, self.dtype, self.mode, **kwargs)  # type: ignore[arg-type]


def get_nested_column_data_types(
    columns: Dict[str, Dict[str, Any]],
    constraints: Optional[Dict[str, str]] = None,
) -> Dict[str, Dict[str, str]]:
    """
    columns:
        * Dictionary where keys are of flat columns names and values are dictionary of column attributes
        * column names with "." indicate a nested column within a STRUCT type
        * e.g. {"a": {"name": "a", "data_type": "string", ...}}
    constraints:
        * Dictionary where keys are flat column names and values are rendered constraints for the column
        * If provided, rendered column is included in returned "data_type" values.
    returns:
        * Dictionary where keys are root column names and values are corresponding nested data_type values.
        * Fields other than "name" and "data_type" are __not__ preserved in the return value for nested columns.
        * Fields other than "name" and "data_type" are preserved in the return value for flat columns.

    Example:
    columns: {
        "a": {"name": "a", "data_type": "string", "description": ...},
        "b.nested": {"name": "b.nested", "data_type": "string"},
        "b.nested2": {"name": "b.nested2", "data_type": "string"}
        }

    returns: {
        "a": {"name": "a", "data_type": "string"},
        "b": {"name": "b": "data_type": "struct<nested string, nested2 string>}
    }
    """
    constraints = constraints or {}

    nested_column_data_types: Dict[str, Union[str, Dict]] = {}
    for column in columns.values():
        _update_nested_column_data_types(
            column["name"],
            column["data_type"],
            constraints.get(column["name"]),
            nested_column_data_types,
        )

    formatted_nested_column_data_types: Dict[str, Dict[str, str]] = {}
    for column_name, unformatted_column_type in nested_column_data_types.items():
        formatted_nested_column_data_types[column_name] = {
            "name": column_name,
            "data_type": _format_nested_data_type(unformatted_column_type),
        }

    # add column configs back to flat columns
    for column_name in formatted_nested_column_data_types:
        if column_name in columns:
            formatted_nested_column_data_types[column_name].update(
                {
                    k: v
                    for k, v in columns[column_name].items()
                    if k not in formatted_nested_column_data_types[column_name]
                }
            )

    return formatted_nested_column_data_types


def _update_nested_column_data_types(
    column_name: str,
    column_data_type: str,
    column_rendered_constraint: Optional[str],
    nested_column_data_types: Dict[str, Union[str, Dict]],
) -> None:
    """
    Recursively update nested_column_data_types given a column_name, column_data_type, and optional column_rendered_constraint.

    Examples:
    >>> nested_column_data_types = {}
    >>> BigQueryAdapter._update_nested_column_data_types("a", "string", "not_null", nested_column_data_types)
    >>> nested_column_data_types
    {"a": "string not null"}
    >>> BigQueryAdapter._update_nested_column_data_types("b.c", "string", "not_null", nested_column_data_types)
    >>> nested_column_data_types
    {"a": "string not null", "b": {"c": "string not null"}}
    >>> BigQueryAdapter._update_nested_column_data_types("b.d", "string", None, nested_column_data_types)
    >>> nested_column_data_types
    {"a": "string not null", "b": {"c": "string not null", "d": "string"}}
    """
    column_name_parts = column_name.split(".")
    root_column_name = column_name_parts[0]

    if len(column_name_parts) == 1:
        # Base case: column is not nested - store its data_type concatenated with constraint if provided.
        nested_column_data_types[root_column_name] = (
            column_data_type
            if column_rendered_constraint is None
            else f"{column_data_type} {column_rendered_constraint}"
        )
    else:
        # Initialize nested dictionary
        if root_column_name not in nested_column_data_types:
            nested_column_data_types[root_column_name] = {}

        # Recursively process rest of remaining column name
        remaining_column_name = ".".join(column_name_parts[1:])
        remaining_column_data_types = nested_column_data_types[root_column_name]
        assert isinstance(remaining_column_data_types, dict)  # keeping mypy happy
        _update_nested_column_data_types(
            remaining_column_name,
            column_data_type,
            column_rendered_constraint,
            remaining_column_data_types,
        )


def _format_nested_data_type(unformatted_nested_data_type: Union[str, Dict[str, Any]]) -> str:
    """
    Recursively format a (STRUCT) data type given an arbitrarily nested data type structure.

    Examples:
    >>> BigQueryAdapter._format_nested_data_type("string")
    'string'
    >>> BigQueryAdapter._format_nested_data_type({'c': 'string not_null', 'd': 'string'})
    'struct<c string not_null, d string>'
    >>> BigQueryAdapter._format_nested_data_type({'c': 'string not_null', 'd': {'e': 'string'}})
    'struct<c string not_null, d struct<e string>>'
    """
    if isinstance(unformatted_nested_data_type, str):
        return unformatted_nested_data_type
    else:
        formatted_nested_types = [
            f"{column_name} {_format_nested_data_type(column_type)}"
            for column_name, column_type in unformatted_nested_data_type.items()
        ]
        return f"""struct<{", ".join(formatted_nested_types)}>"""
