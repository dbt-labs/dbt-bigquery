from dataclasses import dataclass
from typing import Any, Dict, List, Optional

import dbt_common.exceptions
from dbt.adapters.relation_configs import RelationConfigChange
from dbt.adapters.contracts.relation import RelationConfig
from dbt_common.dataclass_schema import dbtClassMixin, ValidationError
from google.cloud.bigquery.table import Table as BigQueryTable


@dataclass
class PartitionConfig(dbtClassMixin):
    field: str
    data_type: str = "date"
    granularity: str = "day"
    range: Optional[Dict[str, Any]] = None
    time_ingestion_partitioning: bool = False
    copy_partitions: bool = False

    PARTITION_DATE = "_PARTITIONDATE"
    PARTITION_TIME = "_PARTITIONTIME"

    def data_type_for_partition(self):
        """Return the data type of partitions for replacement.
        When time_ingestion_partitioning is enabled, the data type supported are date & timestamp.
        """
        if not self.time_ingestion_partitioning:
            return self.data_type

        return "date" if self.data_type == "date" else "timestamp"

    def reject_partition_field_column(self, columns: List[Any]) -> List[str]:
        return [c for c in columns if not c.name.upper() == self.field.upper()]

    def data_type_should_be_truncated(self):
        """Return true if the data type should be truncated instead of cast to the data type."""
        return not (
            self.data_type == "int64" or (self.data_type == "date" and self.granularity == "day")
        )

    def time_partitioning_field(self) -> str:
        """Return the time partitioning field name based on the data type.
        The default is _PARTITIONTIME, but for date it is _PARTITIONDATE
        else it will fail statements for type mismatch."""
        if self.data_type == "date":
            return self.PARTITION_DATE
        else:
            return self.PARTITION_TIME

    def insertable_time_partitioning_field(self) -> str:
        """Return the insertable time partitioning field name based on the data type.
        Practically, only _PARTITIONTIME works so far.
        The function is meant to keep the call sites consistent as it might evolve."""
        return self.PARTITION_TIME

    def render(self, alias: Optional[str] = None):
        column: str = (
            self.field if not self.time_ingestion_partitioning else self.time_partitioning_field()
        )
        if alias:
            column = f"{alias}.{column}"

        if self.data_type_should_be_truncated():
            return f"{self.data_type}_trunc({column}, {self.granularity})"
        else:
            return column

    def render_wrapped(self, alias: Optional[str] = None):
        """Wrap the partitioning column when time involved to ensure it is properly cast to matching time."""
        # if data type is going to be truncated, no need to wrap
        if (
            self.data_type in ("date", "timestamp", "datetime")
            and not self.data_type_should_be_truncated()
            and not (
                self.time_ingestion_partitioning and self.data_type == "date"
            )  # _PARTITIONDATE is already a date
        ):
            return f"{self.data_type}({self.render(alias)})"
        else:
            return self.render(alias)

    @classmethod
    def parse(cls, raw_partition_by) -> Optional["PartitionConfig"]:
        if raw_partition_by is None:
            return None
        try:
            cls.validate(raw_partition_by)
            return cls.from_dict(
                {
                    key: (value.lower() if isinstance(value, str) else value)
                    for key, value in raw_partition_by.items()
                }
            )
        except ValidationError as exc:
            raise dbt_common.exceptions.base.DbtValidationError(
                "Could not parse partition config"
            ) from exc
        except TypeError:
            raise dbt_common.exceptions.CompilationError(
                f"Invalid partition_by config:\n"
                f"  Got: {raw_partition_by}\n"
                f'  Expected a dictionary with "field" and "data_type" keys'
            )

    @classmethod
    def parse_model_node(cls, relation_config: RelationConfig) -> Dict[str, Any]:
        """
        Parse model node into a raw config for `PartitionConfig.parse`

        - Note:
            This doesn't currently collect `time_ingestion_partitioning` and `copy_partitions`
            because this was built for materialized views, which do not support those settings.
        """
        config_dict: Dict[str, Any] = relation_config.config.extra.get("partition_by")
        if "time_ingestion_partitioning" in config_dict:
            del config_dict["time_ingestion_partitioning"]
        if "copy_partitions" in config_dict:
            del config_dict["copy_partitions"]
        return config_dict

    @classmethod
    def parse_bq_table(cls, table: BigQueryTable) -> Dict[str, Any]:
        """
        Parse the BQ Table object into a raw config for `PartitionConfig.parse`

        - Note:
            This doesn't currently collect `time_ingestion_partitioning` and `copy_partitions`
            because this was built for materialized views, which do not support those settings.
        """
        if time_partitioning := table.time_partitioning:
            field_types = {field.name: field.field_type.lower() for field in table.schema}
            config_dict = {
                "field": time_partitioning.field,
                "data_type": field_types[time_partitioning.field],
                "granularity": time_partitioning.type_,
            }

        elif range_partitioning := table.range_partitioning:
            config_dict = {
                "field": range_partitioning.field,
                "data_type": "int64",
                "range": {
                    "start": range_partitioning.range_.start,
                    "end": range_partitioning.range_.end,
                    "interval": range_partitioning.range_.interval,
                },
            }

        else:
            config_dict = {}

        return config_dict


@dataclass(frozen=True, eq=True, unsafe_hash=True)
class BigQueryPartitionConfigChange(RelationConfigChange):
    context: Optional[Any] = None

    @property
    def requires_full_refresh(self) -> bool:
        return True
