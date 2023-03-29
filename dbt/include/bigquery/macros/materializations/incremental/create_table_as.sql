{% macro bq_create_table_as(is_time_ingestion_partitioning, temporary, relation, compiled_code, language='sql') %}

    {% if is_time_ingestion_partitioning and language == 'python' %}
        {% do exceptions.raise_compiler_error(
            "Python models do not support ingestion time partitioning"
        ) %}
    {% endif %}

    {% if is_time_ingestion_partitioning and language == 'sql' %}
        {#-- Create the table before inserting data as ingestion time partitioned tables can't be created with the transformed data --#}
        {% do run_query(create_ingestion_time_partitioned_table_as_sql(temporary, relation, compiled_code)) %}
        {{ return(bq_insert_into_ingestion_time_partitioned_table_sql(relation, compiled_code)) }}
    {% else %}
        {{ return(create_table_as(temporary, relation, compiled_code, language)) }}
    {% endif %}

{% endmacro %}
