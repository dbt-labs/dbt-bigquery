{% macro bigquery__create_table_as(temporary, relation, compiled_code, language='sql') %}
    {% if language == 'sql' %}
        {% create_sql_table(temporary, compiled_code) %}
    {% elif language == 'python' %}
        {#--
        N.B. Python models _can_ write to temp views HOWEVER they use a different session
        and have already expired by the time they need to be used (I.E. in merges for incremental models)

        TODO: Deep dive into spark sessions to see if we can reuse a single session for an entire
        dbt invocation.
         --#}
        {{ py_write_table(
            compiled_code=compiled_code,
            target_relation=relation.quote(database=False,
                schema=False,
                identifier=False
            )
        ) }}
    {% else %}
        {% do exceptions.raise_compiler_error("bigquery__create_table_as macro didn't get supported language, it got %s" % language) %}
    {% endif %}

{% endmacro %}


{% macro create_sql_table(temporary) %}
    {% set contract = config.get('contract') %}
    {% if contract.enforced %}
        {{ create_sql_table_with_contract(temporary, sql) }}
    {% else %}
        {{ create_sql_table_with_no_contract(temporary, sql) }}
    {% endif %}
{% endmacro %}


{% macro create_sql_table_with_contract(temporary, sql) %}
    {{ get_assert_columns_equivalent(sql) }}
    {% set sql = get_select_subquery(sql) %}
    {% set raw_partition_by = config.get('partition_by', none) %}
    {% set partition_config = adapter.parse_partition_by(raw_partition_by) %}
    {% set raw_cluster_by = config.get('cluster_by', none) %}
    {% set sql_header = config.get('sql_header', none) %}

    {{ sql_header if sql_header is not none }}

    create or replace table {{ relation }}
        {{ get_columns_spec_ddl() }}
        {{ partition_by(partition_config) }}
        {{ cluster_by(raw_cluster_by) }}
        {{ bigquery_table_options(config, model, temporary) }}
    as (
        {{ sql }}
    );
{% endmacro %}


{% macro create_sql_table_with_no_contract(temporary, sql) %}
    {% set raw_partition_by = config.get('partition_by', none) %}
    {% set partition_config = adapter.parse_partition_by(raw_partition_by) %}
    {% set raw_cluster_by = config.get('cluster_by', none) %}
    {% set sql_header = config.get('sql_header', none) %}

    {{ sql_header if sql_header is not none }}

    create or replace table {{ relation }}
        {{ partition_by(partition_config) }}
        {{ cluster_by(raw_cluster_by) }}
        {{ bigquery_table_options(config, model, temporary) }}
    as (
        {{ sql }}
    );
{% endmacro %}


-- TODO dataproc requires a temp bucket to perform BQ write
-- this is hard coded to internal testing ATM. need to adjust to render
-- or find another way around
{% macro py_write_table(compiled_code, target_relation) %}
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('smallTest').getOrCreate()

spark.conf.set("viewsEnabled","true")
spark.conf.set("temporaryGcsBucket","{{target.gcs_bucket}}")

{{ compiled_code }}
dbt = dbtObj(spark.read.format("bigquery").load)
df = model(dbt, spark)

# COMMAND ----------
# this is materialization code dbt generated, please do not modify

import pyspark
# make sure pandas exists before using it
try:
  import pandas
  pandas_available = True
except ImportError:
  pandas_available = False

# make sure pyspark.pandas exists before using it
try:
  import pyspark.pandas
  pyspark_pandas_api_available = True
except ImportError:
  pyspark_pandas_api_available = False

# make sure databricks.koalas exists before using it
try:
  import databricks.koalas
  koalas_available = True
except ImportError:
  koalas_available = False

# preferentially convert pandas DataFrames to pandas-on-Spark or Koalas DataFrames first
# since they know how to convert pandas DataFrames better than `spark.createDataFrame(df)`
# and converting from pandas-on-Spark to Spark DataFrame has no overhead
if pyspark_pandas_api_available and pandas_available and isinstance(df, pandas.core.frame.DataFrame):
  df = pyspark.pandas.frame.DataFrame(df)
elif koalas_available and pandas_available and isinstance(df, pandas.core.frame.DataFrame):
  df = databricks.koalas.frame.DataFrame(df)

# convert to pyspark.sql.dataframe.DataFrame
if isinstance(df, pyspark.sql.dataframe.DataFrame):
  pass  # since it is already a Spark DataFrame
elif pyspark_pandas_api_available and isinstance(df, pyspark.pandas.frame.DataFrame):
  df = df.to_spark()
elif koalas_available and isinstance(df, databricks.koalas.frame.DataFrame):
  df = df.to_spark()
elif pandas_available and isinstance(df, pandas.core.frame.DataFrame):
  df = spark.createDataFrame(df)
else:
  msg = f"{type(df)} is not a supported type for dbt Python materialization"
  raise Exception(msg)

df.write \
  .mode("overwrite") \
  .format("bigquery") \
  .option("writeMethod", "direct").option("writeDisposition", 'WRITE_TRUNCATE') \
  .save("{{target_relation}}")
{% endmacro %}


{% macro bigquery_table_options(config, node, temporary) %}
    {% set opts = adapter.get_table_options(config, node, temporary) %}
    {% do return(bigquery_options(opts)) %}
{% endmacro %}
