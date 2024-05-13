{% macro bigquery__get_relation_last_modified(information_schema, relations) -%}

  {%- call statement('last_modified', fetch_result=True) -%}
        select table_schema as schema,
        table_name as identifier,
        storage_last_modified_time as last_modified,
               {{ current_timestamp() }} as snapshotted_at
        -- TODO: stop hardcoding this
        from `region-us`.INFORMATION_SCHEMA.TABLE_STORAGE
        -- TODO: preprocessing unique schemas would make the query size smaller
        where (upper(table_schema) IN (
            {%- for relation in relations -%}
            upper('{{ relation.schema }}'){%- if not loop.last %},{% endif -%}
            {%- endfor -%}
            )
        )
        and (
          {%- for relation in relations -%}
            (upper(table_schema) = upper('{{ relation.schema }}') and
             upper(table_name) = upper('{{ relation.identifier }}')){%- if not loop.last %} or {% endif -%}
          {%- endfor -%}
        )
  {%- endcall -%}

  {{ return(load_result('last_modified')) }}

{% endmacro %}
