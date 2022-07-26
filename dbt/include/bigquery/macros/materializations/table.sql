{% materialization table, adapter='bigquery' -%}

  {%- set language = config.get('language') -%}
  {%- set identifier = model['alias'] -%}
  {%- set old_relation = adapter.get_relation(database=database, schema=schema, identifier=identifier) -%}
  {%- set exists_not_as_table = (old_relation is not none and not old_relation.is_table) -%}
  {%- set target_relation = api.Relation.create(database=database, schema=schema, identifier=identifier, type='table') -%}

  -- grab current tables grants config for comparision later on
  {%- set grant_config = config.get('grants') -%}

  {{ run_hooks(pre_hooks) }}

  {#
      We only need to drop this thing if it is not a table.
      If it _is_ already a table, then we can overwrite it without downtime
      Unlike table -> view, no need for `--full-refresh`: dropping a view is no big deal
  #}
  {%- if exists_not_as_table -%}
      {{ adapter.drop_relation(old_relation) }}
  {%- endif -%}

  -- build model
  {%- set raw_partition_by = config.get('partition_by', none) -%}
  {%- set partition_by = adapter.parse_partition_by(raw_partition_by) -%}
  {%- set cluster_by = config.get('cluster_by', none) -%}
  {% if not adapter.is_replaceable(old_relation, partition_by, cluster_by) %}
    {% do log("Hard refreshing " ~ old_relation ~ " because it is not replaceable") %}
    {% do adapter.drop_relation(old_relation) %}
  {% endif %}

  -- build model
  {%- call statement('main', language=language) -%}
    {{ create_table_as(False, target_relation, compiled_code, language) }}
  {%- endcall -%}

  {{ run_hooks(post_hooks) }}

  {% set should_revoke = should_revoke(old_relation, full_refresh_mode=True) %}
  {% do apply_grants(target_relation, grant_config, should_revoke) %}

  {% do persist_docs(target_relation, model) %}

  {{ return({'relations': [target_relation]}) }}

{% endmaterialization %}

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


df.write \
  .mode("overwrite") \
  .format("bigquery") \
  .option("writeMethod", "direct") \
  .save("{{target_relation}}")
{% endmacro %}
