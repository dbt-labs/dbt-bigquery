{% materialization copy, adapter='bigquery' -%}

  {# Setup #}
    -- grab current tables grants config for comparision later on
  {# TODO: this is a guess I need to circle back to - does the BQ copy API also copy grants automatically?  If yes it seems like we would want to option to overwrite them if needed? #}
  {%- set  grant_config = config.get('grants') -%}
  {{ run_hooks(pre_hooks) }}

  {% set destination = this.incorporate(type='table') %}

  {# there can be several ref() or source() according to BQ copy API docs #}
  {# cycle over ref() and source() to create source tables array #}
  {% set source_array = [] %}
  {% for ref_table in model.refs %}
    {{ source_array.append(ref(*ref_table)) }}
  {% endfor %}

  {% for src_table in model.sources %}
    {{ source_array.append(source(*src_table)) }}
  {% endfor %}

  {# Call adapter copy_table function #}
  {%- set result_str = adapter.copy_table(
      source_array,
      destination,
      config.get('copy_materialization', default = 'table')) -%}

  {{ store_result('main', response=result_str) }}

  {# Clean up #}
  {{ run_hooks(post_hooks) }}
  {%- do apply_grants(target_relation, grant_config) -%}
  {{ adapter.commit() }}

  {{ return({'relations': [destination]}) }}
{%- endmaterialization %}
