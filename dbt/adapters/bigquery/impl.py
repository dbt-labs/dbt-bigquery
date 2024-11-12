from dataclasses import dataclass
from datetime import datetime
from multiprocessing.context import SpawnContext
import threading
from typing import (
    Any,
    Dict,
    FrozenSet,
    Iterable,
    List,
    Optional,
    Set,
    Tuple,
    TYPE_CHECKING,
    Type,
    Union,
)

from google.api_core.exceptions import Forbidden, NotFound
from google.cloud.bigquery import AccessEntry, Table as BigQueryTable
import pytz

from dbt_common.contracts.constraints import (
    ColumnLevelConstraint,
    ConstraintType,
    ModelLevelConstraint,
)
from dbt_common.dataclass_schema import dbtClassMixin
from dbt_common.events.contextvars import get_node_info
from dbt_common.events.functions import fire_event
from dbt_common.exceptions import CompilationError, DbtRuntimeError
import dbt_common.exceptions.base
from dbt_common.utils import AttrDict, filter_null_values
from dbt.adapters.base import (
    AdapterConfig,
    BaseAdapter,
    BaseRelation,
    ConstraintSupport,
    PythonJobHelper,
    RelationType,
    SchemaSearchMap,
    available,
)
from dbt.adapters.base.impl import FreshnessResponse, _parse_callback_empty_table
from dbt.adapters.cache import _make_ref_key_dict
from dbt.adapters.capability import Capability, CapabilityDict, CapabilitySupport, Support
from dbt.adapters.contracts.connection import AdapterRequiredConfig, AdapterResponse
from dbt.adapters.contracts.macros import MacroResolverProtocol
from dbt.adapters.contracts.relation import RelationConfig
from dbt.adapters.events.logging import AdapterLogger
from dbt.adapters.events.types import SQLQuery, SchemaCreation, SchemaDrop

from dbt.adapters.bigquery.column import BigQueryColumn, get_nested_column_data_types
from dbt.adapters.bigquery.connections import BigQueryAdapterResponse, BigQueryConnectionManager
from dbt.adapters.bigquery.dataset import add_access_entry_to_dataset, is_access_entry_in_dataset
from dbt.adapters.bigquery.python_submissions import (
    ClusterDataprocHelper,
    ServerlessDataProcHelper,
)
from dbt.adapters.bigquery.relation import BigQueryRelation
from dbt.adapters.bigquery.relation_configs import (
    BigQueryBaseRelationConfig,
    BigQueryMaterializedViewConfig,
    PartitionConfig,
)
from dbt.adapters.bigquery.retry import RetryFactory
from dbt.adapters.bigquery.services import bigquery, macros

if TYPE_CHECKING:
    # Indirectly imported via agate_helper, which is lazy loaded further downfile.
    # Used by mypy for earlier type hints.
    import agate


logger = AdapterLogger("BigQuery")


_dataset_lock = threading.Lock()


@dataclass
class GrantTarget(dbtClassMixin):
    dataset: str
    project: str

    def render(self):
        return f"{self.project}.{self.dataset}"


def _stub_relation(*args, **kwargs):
    return BigQueryRelation.create(
        database="", schema="", identifier="", quote_policy={}, type=BigQueryRelation.Table
    )


@dataclass
class BigqueryConfig(AdapterConfig):
    cluster_by: Optional[Union[List[str], str]] = None
    partition_by: Optional[Dict[str, Any]] = None
    kms_key_name: Optional[str] = None
    labels: Optional[Dict[str, str]] = None
    partitions: Optional[List[str]] = None
    grant_access_to: Optional[List[Dict[str, str]]] = None
    hours_to_expiration: Optional[int] = None
    require_partition_filter: Optional[bool] = None
    partition_expiration_days: Optional[int] = None
    merge_update_columns: Optional[str] = None
    enable_refresh: Optional[bool] = None
    refresh_interval_minutes: Optional[int] = None
    max_staleness: Optional[str] = None
    enable_list_inference: Optional[bool] = None
    intermediate_format: Optional[str] = None


class BigQueryAdapter(BaseAdapter):
    RELATION_TYPES = {
        "TABLE": RelationType.Table,
        "VIEW": RelationType.View,
        "MATERIALIZED_VIEW": RelationType.MaterializedView,
        "EXTERNAL": RelationType.External,
    }

    Relation = BigQueryRelation
    Column = BigQueryColumn
    ConnectionManager = BigQueryConnectionManager

    AdapterSpecificConfigs = BigqueryConfig

    CONSTRAINT_SUPPORT = {
        ConstraintType.check: ConstraintSupport.NOT_SUPPORTED,
        ConstraintType.not_null: ConstraintSupport.ENFORCED,
        ConstraintType.unique: ConstraintSupport.NOT_SUPPORTED,
        ConstraintType.primary_key: ConstraintSupport.NOT_ENFORCED,
        ConstraintType.foreign_key: ConstraintSupport.NOT_ENFORCED,
    }

    _capabilities: CapabilityDict = CapabilityDict(
        {
            Capability.TableLastModifiedMetadata: CapabilitySupport(support=Support.Full),
            Capability.SchemaMetadataByRelations: CapabilitySupport(support=Support.Full),
        }
    )

    def __init__(self, config: AdapterRequiredConfig, mp_context: SpawnContext) -> None:
        super().__init__(config, mp_context)
        self.connections: BigQueryConnectionManager = self.connections
        self.retry = RetryFactory(config.credentials)

    ###
    # Implementations of abstract methods
    ###

    @available.parse(_parse_callback_empty_table)
    def execute(
        self,
        sql: str,
        auto_begin: bool = False,
        fetch: bool = False,
        limit: Optional[int] = None,
    ) -> Tuple[AdapterResponse, "agate.Table"]:
        # TODO: this currently points to connections.execute; move to bigquery service
        return super().execute(sql, auto_begin, fetch, limit)

    def validate_sql(self, sql: str) -> AdapterResponse:
        """Submit the given SQL to the engine for validation, but not execution.

        This submits the query with the `dry_run` flag set True.

        :param str sql: The sql to validate
        """
        connection = self.connections.get_thread_connection()
        sql = self.connections._add_query_comment(sql)
        config = self.connections.query_job_defaults()
        config["dry_run"] = True

        fire_event(SQLQuery(conn_name=connection.name, sql=sql, node_info=get_node_info()))

        query_job, _ = bigquery.execute(
            connection.handle,
            sql,
            config,
            create_timeout=self.retry.job_creation_timeout(),
            execute_timeout=self.retry.job_execution_timeout(),
            job_id=self.connections.generate_job_id(),
        )
        return bigquery.query_job_response(connection.handle, query_job)

    @available.parse(lambda *a, **k: [])
    def get_column_schema_from_query(self, sql: str) -> List[BigQueryColumn]:
        """Get a list of the column names and data types from the given sql.

        :param str sql: The sql to execute.
        :return: List[BigQueryColumn]
        """
        connection = self.connections.get_thread_connection()
        sql = self.connections._add_query_comment(sql)
        config = self.connections.query_job_defaults()

        fire_event(SQLQuery(conn_name=connection.name, sql=sql, node_info=get_node_info()))

        _, iterator = bigquery.execute(
            connection.handle,
            sql,
            config,
            create_timeout=self.retry.job_creation_timeout(),
            execute_timeout=self.retry.job_execution_timeout(),
            job_id=self.connections.generate_job_id(),
        )

        return bigquery.columns(iterator.schema)

    def get_partitions_metadata(self, table):
        legacy_sql = (
            "SELECT * FROM ["
            + table.project
            + ":"
            + table.dataset
            + "."
            + table.identifier
            + "$__PARTITIONS_SUMMARY__]"
        )

        sql = self.connections._add_query_comment(legacy_sql)
        # auto_begin is ignored on bigquery, and only included for consistency
        _, iterator = self.connections.raw_execute(sql, use_legacy_sql=True)

        column_names = [field.name for field in iterator.schema]

        from dbt_common.clients import agate_helper

        return agate_helper.table_from_data_flat(iterator, column_names)

    def _get_catalog_schemas(self, relation_config: Iterable[RelationConfig]) -> SchemaSearchMap:
        candidates = super()._get_catalog_schemas(relation_config)
        db_schemas: Dict[str, Set[str]] = {}
        result = SchemaSearchMap()

        for candidate, schemas in candidates.items():
            database = candidate.database
            if database not in db_schemas:
                db_schemas[database] = set(self.list_schemas(database))
            if candidate.schema in db_schemas[database]:
                result[candidate] = schemas
            else:
                logger.debug(
                    "Skipping catalog for {}.{} - schema does not exist".format(
                        database, candidate.schema
                    )
                )
        return result

    @classmethod
    def date_function(cls) -> str:
        return "CURRENT_TIMESTAMP()"

    @classmethod
    def is_cancelable(cls) -> bool:
        return True

    @available
    def list_schemas(self, database: str) -> List[str]:
        connection = self.connections.get_thread_connection()
        retry = self.retry.reopen_with_deadline(connection)
        return bigquery.list_schemas(connection.handle, database, retry)

    @available.parse(lambda *a, **k: False)
    def check_schema_exists(self, database: str, schema: str) -> bool:
        client = self.connections.bigquery_client()
        relation = self.Relation.create(database, schema)
        return bigquery.schema_exists(client, relation)

    def drop_relation(self, relation: BigQueryRelation) -> None:
        is_cached = self._schema_is_cached(relation.database, relation.schema)
        if is_cached:
            self.cache_dropped(relation)

        conn = self.connections.get_thread_connection()

        table_ref = bigquery.table_ref(relation)

        # mimic "drop if exists" functionality that's ubiquitous in most sql implementations
        conn.handle.delete_table(table_ref, not_found_ok=True)

    def truncate_relation(self, relation: BigQueryRelation) -> None:
        raise dbt_common.exceptions.base.NotImplementedError(
            "`truncate` is not implemented for this adapter!"
        )

    def rename_relation(
        self, from_relation: BigQueryRelation, to_relation: BigQueryRelation
    ) -> None:
        conn = self.connections.get_thread_connection()
        client = conn.handle

        if from_relation.type == RelationType.View or to_relation.type == RelationType.View:
            raise DbtRuntimeError("Renaming of views is not currently supported in BigQuery")

        self.cache_renamed(from_relation, to_relation)
        bigquery.copy_table(client, from_relation, to_relation, "table")
        bigquery.drop_table(client, from_relation)

    def get_columns_in_relation(self, relation: BigQueryRelation) -> List[BigQueryColumn]:
        connection = self.connections.get_thread_connection()
        retry = self.retry.reopen_with_deadline(connection)
        return bigquery.get_columns(connection.handle, relation, retry)

    def expand_column_types(self, goal: BigQueryRelation, current: BigQueryRelation) -> None:
        # This is a no-op on BigQuery
        pass

    @available.parse_list
    def list_relations_without_caching(
        self, schema_relation: BigQueryRelation
    ) -> List[BigQueryRelation]:
        connection = self.connections.get_thread_connection()
        client = connection.handle

        dataset_ref = bigquery.dataset_ref(schema_relation)

        all_tables = client.list_tables(
            dataset_ref,
            # BigQuery paginates tables by alphabetizing them, and using
            # the name of the last table on a page as the key for the
            # next page. If that key table gets dropped before we run
            # list_relations, then this will 404. So, we avoid this
            # situation by making the page size sufficiently large.
            # see: https://github.com/dbt-labs/dbt/issues/726
            # TODO: cache the list of relations up front, and then we
            #       won't need to do this
            max_results=100000,
        )

        # This will 404 if the dataset does not exist. This behavior mirrors
        # the implementation of list_relations for other adapters
        try:
            return [bigquery.base_relation(table) for table in all_tables]  # type: ignore[misc]
        except NotFound:
            return []
        except Forbidden as exc:
            logger.debug("list_relations_without_caching error: {}".format(str(exc)))
            return []

    def expand_target_column_types(
        self, from_relation: BigQueryRelation, to_relation: BigQueryRelation
    ) -> None:
        # This is a no-op on BigQuery
        pass

    def _make_match_kwargs(self, database: str, schema: str, identifier: str) -> Dict[str, str]:
        return filter_null_values(
            {
                "database": database,
                "identifier": identifier,
                "schema": schema,
            }
        )

    def get_relation(
        self, database: str, schema: str, identifier: str
    ) -> Optional[BigQueryRelation]:
        if self._schema_is_cached(database, schema):
            # if it's in the cache, use the parent's model of going through
            # the relations cache and picking out the relation
            return super().get_relation(database=database, schema=schema, identifier=identifier)

        connection = self.connections.get_thread_connection()
        relation = self.Relation.create(database, schema, identifier)
        retry = self.retry.reopen_with_deadline(connection)
        table = bigquery.get_table(connection.handle, relation, retry)
        return bigquery.base_relation(table)

    def create_schema(self, relation: BigQueryRelation) -> None:
        # use SQL 'create schema'
        fire_event(SchemaCreation(relation=_make_ref_key_dict(relation.without_identifier())))
        macros.create_schema(relation)
        # we can't update the cache here, as if the schema already existed we
        # don't want to (incorrectly) say that it's empty

    def drop_schema(self, relation: BigQueryRelation) -> None:
        # still use a client method, rather than SQL 'drop schema ... cascade'
        connection = self.connections.get_thread_connection()
        retry = self.retry.reopen_with_deadline(connection)

        fire_event(SchemaDrop(relation=_make_ref_key_dict(relation)))
        bigquery.drop_schema(connection.handle, relation, retry)
        self.cache.drop_schema(relation.database, relation.schema)

    @classmethod
    def quote(cls, identifier: str) -> str:
        return "`{}`".format(identifier)

    @classmethod
    def convert_text_type(cls, agate_table: "agate.Table", col_idx: int) -> str:
        return "string"

    @classmethod
    def convert_number_type(cls, agate_table: "agate.Table", col_idx: int) -> str:
        import agate

        decimals = agate_table.aggregate(agate.MaxPrecision(col_idx))  # type: ignore[attr-defined]
        return "float64" if decimals else "int64"

    @classmethod
    def convert_integer_type(cls, agate_table: "agate.Table", col_idx: int) -> str:
        return "int64"

    @classmethod
    def convert_boolean_type(cls, agate_table: "agate.Table", col_idx: int) -> str:
        return "bool"

    @classmethod
    def convert_datetime_type(cls, agate_table: "agate.Table", col_idx: int) -> str:
        return "datetime"

    @classmethod
    def convert_date_type(cls, agate_table: "agate.Table", col_idx: int) -> str:
        return "date"

    @classmethod
    def convert_time_type(cls, agate_table: "agate.Table", col_idx: int) -> str:
        return "time"

    ###
    # Implementation details
    ###

    @available.parse(lambda *a, **k: {})
    @classmethod
    def nest_column_data_types(
        cls,
        columns: Dict[str, Dict[str, Any]],
        constraints: Optional[Dict[str, str]] = None,
    ) -> Dict[str, Dict[str, Optional[str]]]:
        return get_nested_column_data_types(columns, constraints)

    @available.parse(lambda *a, **k: [])
    def add_time_ingestion_partition_column(self, partition_by, columns) -> List[BigQueryColumn]:
        """Add time ingestion partition column to columns list"""
        columns.append(
            self.Column(
                partition_by.insertable_time_partitioning_field(),
                partition_by.data_type,
                None,
                "NULLABLE",
            )
        )
        return columns

    def execute_macro(
        self,
        macro_name: str,
        macro_resolver: Optional[MacroResolverProtocol] = None,
        project: Optional[str] = None,
        context_override: Optional[Dict[str, Any]] = None,
        kwargs: Optional[Dict[str, Any]] = None,
        needs_conn: bool = False,
    ) -> AttrDict:
        macro = macros.macro(
            macro_name,
            macro_resolver or self._macro_resolver,
            self._macro_context_generator,
            self.config,
            project,
            context_override or {},
        )

        if needs_conn:
            connection = self.connections.get_thread_connection()
            self.connections.open(connection)

        with self.connections.exception_handler(f"macro {macro_name}"):
            result = macro(**kwargs)
        return result

    @classmethod
    def _catalog_filter_table(
        cls, table: "agate.Table", used_schemas: FrozenSet[Tuple[str, str]]
    ) -> "agate.Table":
        table = table.rename(
            column_names={col.name: col.name.replace("__", ":") for col in table.columns}
        )
        return super()._catalog_filter_table(table, used_schemas)

    def calculate_freshness_from_metadata(
        self,
        source: BigQueryRelation,
        macro_resolver: Optional[MacroResolverProtocol] = None,
    ) -> Tuple[Optional[AdapterResponse], FreshnessResponse]:
        client = self.connections.bigquery_client()
        table = bigquery.get_table(client, source)
        snapshot = datetime.now(tz=pytz.UTC)
        return None, FreshnessResponse(
            max_loaded_at=table.modified,
            snapshotted_at=snapshot,
            age=(snapshot - table.modified).total_seconds(),
        )

    def timestamp_add_sql(self, add_to: str, number: int = 1, interval: str = "hour") -> str:
        return f"timestamp_add({add_to}, interval {number} {interval})"

    def string_add_sql(
        self,
        add_to: str,
        value: str,
        location="append",
    ) -> str:
        if location == "append":
            return f"concat({add_to}, '{value}')"
        elif location == "prepend":
            return f"concat('{value}', {add_to})"
        else:
            raise DbtRuntimeError(f'Got an unexpected location value of "{location}"')

    def get_rows_different_sql(
        self,
        relation_a: BigQueryRelation,
        relation_b: BigQueryRelation,
        column_names: Optional[List[str]] = None,
        except_operator="EXCEPT DISTINCT",
    ) -> str:
        return super().get_rows_different_sql(
            relation_a=relation_a,
            relation_b=relation_b,
            column_names=column_names,
            except_operator=except_operator,
        )

    @property
    def python_submission_helpers(self) -> Dict[str, Type[PythonJobHelper]]:
        return {
            "cluster": ClusterDataprocHelper,
            "serverless": ServerlessDataProcHelper,
        }

    @property
    def default_python_submission_method(self) -> str:
        return "serverless"

    def generate_python_submission_response(self, submission_result) -> BigQueryAdapterResponse:
        return BigQueryAdapterResponse(_message="OK")

    @classmethod
    def render_column_constraint(cls, constraint: ColumnLevelConstraint) -> Optional[str]:
        c = super().render_column_constraint(constraint)
        if (
            constraint.type == ConstraintType.primary_key
            or constraint.type == ConstraintType.foreign_key
        ):
            return f"{c} not enforced" if c else None
        return c

    @available
    @classmethod
    def render_raw_columns_constraints(cls, raw_columns: Dict[str, Dict[str, Any]]) -> List:
        rendered_constraints: Dict[str, str] = {}
        for raw_column in raw_columns.values():
            for con in raw_column.get("constraints", None):
                constraint = cls._parse_column_constraint(con)
                rendered_constraint = cls.process_parsed_constraint(
                    constraint, cls.render_column_constraint
                )

                if rendered_constraint:
                    column_name = raw_column["name"]
                    if column_name not in rendered_constraints:
                        rendered_constraints[column_name] = rendered_constraint
                    else:
                        rendered_constraints[column_name] += f" {rendered_constraint}"

        nested_columns = cls.nest_column_data_types(raw_columns, rendered_constraints)
        rendered_column_constraints = [
            f"{cls.quote(column['name']) if column.get('quote') else column['name']} {column['data_type']}"
            for column in nested_columns.values()
        ]
        return rendered_column_constraints

    @classmethod
    def render_model_constraint(cls, constraint: ModelLevelConstraint) -> Optional[str]:
        c = super().render_model_constraint(constraint)
        if constraint.type in [ConstraintType.primary_key, ConstraintType.foreign_key]:
            return f"{c} not enforced" if c else None
        return c

    # ==============================
    # dbt-bigquery specific methods
    # ==============================

    @available.parse(lambda *a, **k: "")
    def copy_table(self, source, destination, materialization):
        if materialization not in ["incremental", "table"]:
            raise CompilationError(
                'Copy table materialization must be "incremental" or "table", but '
                f"config.get('copy_materialization', 'table') was {materialization}"
            )

        client = self.connections.bigquery_client()
        timeout = self.retry.job_execution_timeout(300)
        bigquery.copy_table(client, source, destination, materialization, timeout)
        return f"COPY TABLE with materialization: {materialization}"

    @available.parse(lambda *a, **k: False)
    def get_columns_in_select_sql(self, select_sql: str) -> List[BigQueryColumn]:
        connection = self.connections.get_thread_connection()
        sql = self.connections._add_query_comment(select_sql)
        config = self.connections.query_job_defaults()

        fire_event(SQLQuery(conn_name=connection.name, sql=sql, node_info=get_node_info()))

        return bigquery.columns_from_select(
            connection.handle,
            sql,
            config,
            create_timeout=self.retry.job_creation_timeout(),
            execute_timeout=self.retry.job_execution_timeout(),
            job_id=self.connections.generate_job_id(),
        )

    @classmethod
    def warning_on_hooks(cls, hook_type):
        msg = "{} is not supported in bigquery and will be ignored"
        logger.info(msg)

    @available
    def add_query(self, sql, auto_begin=True, bindings=None, abridge_sql_log=False):
        if self.nice_connection_name() in ["on-run-start", "on-run-end"]:
            self.warning_on_hooks(self.nice_connection_name())
        else:
            raise dbt_common.exceptions.base.NotImplementedError(
                "`add_query` is not implemented for this adapter!"
            )

    ###
    # Special bigquery adapter methods
    ###

    @available.parse(lambda *a, **k: True)
    def is_replaceable(
        self, relation, conf_partition: Optional[PartitionConfig], conf_cluster
    ) -> bool:
        client = self.connections.bigquery_client()
        return bigquery.table_is_replaceable(client, relation, conf_partition, conf_cluster)

    @available
    def parse_partition_by(self, raw_partition_by: Any) -> Optional[PartitionConfig]:
        """
        dbt v0.16.0 expects `partition_by` to be a dictionary where previously
        it was a string. Check the type of `partition_by`, raise error
        or warning if string, and attempt to convert to dict.
        """
        return PartitionConfig.parse(raw_partition_by)

    @staticmethod
    def get_table_ref_from_relation(relation: BaseRelation):
        return bigquery.table_ref(relation)

    @available.parse_none
    def update_table_description(
        self, database: str, schema: str, identifier: str, description: str
    ) -> None:
        connection = self.connections.get_thread_connection()
        relation = self.Relation.create(database, schema, identifier)
        retry = self.retry.reopen_with_deadline(connection)
        bigquery.update_table(connection.handle, relation, retry, {"description": description})

    @available.parse_none
    def update_columns(
        self, relation: BigQueryRelation, columns: Dict[str, Dict[str, Any]]
    ) -> None:
        if len(columns) == 0:
            return
        connection = self.connections.get_thread_connection()
        retry = self.retry.reopen_with_deadline(connection)
        bigquery.update_columns(connection.handle, relation, retry, columns)

    @available.parse_none
    def alter_table_add_columns(
        self, relation: BigQueryRelation, columns: List[BigQueryColumn]
    ) -> None:
        logger.debug(f'Adding columns ({columns}) to table {relation}".')

        connection = self.connections.get_thread_connection()
        retry = self.retry.reopen_with_deadline(connection)
        bigquery.add_columns(connection.handle, relation, retry, columns)

    @available.parse_none
    def load_dataframe(
        self,
        database: str,
        schema: str,
        table_name: str,
        agate_table: "agate.Table",
        column_override: Dict[str, str],
        field_delimiter: str,
    ) -> None:
        client = self.connections.bigquery_client()
        relation = self.Relation.create(database, schema, table_name)
        timeout = self.retry.job_execution_timeout(300)

        bigquery.load_table_from_dataframe(
            client,
            agate_table.original_abspath,  # type:ignore
            relation,
            agate_table,
            column_override,
            field_delimiter,
            timeout,
        )

    @available.parse_none
    def upload_file(
        self,
        local_file_path: str,
        database: str,
        table_schema: str,
        table_name: str,
        **kwargs,
    ) -> None:
        client = self.connections.bigquery_client()
        relation = self.Relation.create(database, table_schema, table_name)
        timeout = self.retry.job_execution_timeout(300)

        bigquery.load_table_from_file(
            client,
            local_file_path,
            relation,
            timeout,
            **kwargs,
        )

    @available.parse(lambda *a, **k: {})
    def get_common_options(
        self, config: Dict[str, Any], node: Dict[str, Any], temporary: bool = False
    ) -> Dict[str, Any]:
        return bigquery.common_options(config, node, temporary)

    @available.parse(lambda *a, **k: {})
    def get_table_options(
        self, config: Dict[str, Any], node: Dict[str, Any], temporary: bool
    ) -> Dict[str, Any]:
        return bigquery.table_options(config, node, temporary)

    @available.parse(lambda *a, **k: {})
    def get_view_options(self, config: Dict[str, Any], node: Dict[str, Any]) -> Dict[str, Any]:
        return bigquery.view_options(config, node)

    @available.parse(lambda *a, **k: True)
    def get_bq_table(self, relation: BigQueryRelation) -> Optional[BigQueryTable]:
        connection = self.connections.get_thread_connection()
        retry = self.retry.reopen_with_deadline(connection)
        return bigquery.get_table(connection.handle, relation, retry)

    @available.parse(lambda *a, **k: True)
    def describe_relation(
        self, relation: BigQueryRelation
    ) -> Optional[BigQueryBaseRelationConfig]:
        if relation.type == RelationType.MaterializedView:
            bq_table = self.get_bq_table(relation)
            parser = BigQueryMaterializedViewConfig
        else:
            raise DbtRuntimeError(
                f"The method `BigQueryAdapter.describe_relation` is not implemented "
                f"for the relation type: {relation.type}"
            )
        if bq_table:
            return parser.from_bq_table(bq_table)
        return None

    @available.parse_none
    def grant_access_to(self, entity, entity_type, role, grant_target_dict) -> None:
        """
        Given an entity, grants it access to a dataset.
        """
        client = self.connections.bigquery_client()
        GrantTarget.validate(grant_target_dict)
        grant_target = GrantTarget.from_dict(grant_target_dict)
        if entity_type == "view":
            entity = bigquery.table_ref(entity).to_api_repr()
        with _dataset_lock:
            schema = self.Relation.create(grant_target.project, grant_target.dataset)
            dataset = bigquery.get_dataset(client, schema)
            access_entry = AccessEntry(role, entity_type, entity)
            # only perform update if access entry not in dataset
            if is_access_entry_in_dataset(dataset, access_entry):
                logger.warning(f"Access entry {access_entry} " f"already exists in dataset")
            else:
                dataset = add_access_entry_to_dataset(dataset, access_entry)
                client.update_dataset(dataset, ["access_entries"])

    @available.parse_none
    def get_dataset_location(self, relation: BigQueryRelation) -> str:
        client = self.connections.bigquery_client()
        if dataset := bigquery.get_dataset(client, relation):
            return dataset.location
        return ""

    # This is used by the test suite
    def run_sql_for_tests(self, sql, fetch, conn=None):
        """For the testing framework.
        Run an SQL query on a bigquery adapter. No cursors, transactions,
        etc. to worry about"""

        do_fetch = fetch != "None"
        _, res = self.execute(sql, fetch=do_fetch)

        # convert dataframe to matrix-ish repr
        if fetch == "one":
            return res[0]
        else:
            return list(res)
