{% macro bigquery__get_column_schema_from_query(select_sql) %}
    {% set columns = [] %}
    {% set sql = get_empty_subquery_sql(select_sql) %}
    {% set column_schema = adapter.get_column_schema_from_query(sql) %}
    {% for col in column_schema %}
        {% set column = api.Column.create_from_field(col[1]) %}
        {% do columns.append(column) %}
    {% endfor %}
    {{ return(columns) }}
{% endmacro %}
