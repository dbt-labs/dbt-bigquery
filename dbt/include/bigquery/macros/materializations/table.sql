{% materialization table, adapter='bigquery' -%}

  {%- set identifier = model['alias'] -%}
  {%- set old_relation = adapter.get_relation(database=database, schema=schema, identifier=identifier) -%}
  {%- set exists_not_as_table = (old_relation is not none and not old_relation.is_table) -%}
  {%- set target_relation = api.Relation.create(database=database, schema=schema, identifier=identifier, type='table') -%}

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
  {% call statement('main') -%}
    {{ create_table_as(False, target_relation, sql) }}
  {% endcall -%}

  {{ run_hooks(post_hooks) }}

  {% do persist_docs(target_relation, model) %}

  {{ return({'relations': [target_relation]}) }}

{% endmaterialization %}
