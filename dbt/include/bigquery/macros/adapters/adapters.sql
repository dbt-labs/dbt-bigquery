{% macro bigquery__make_temp_relation(base_relation, suffix='__dbt_tmp') %}
    {% if var('tmp_relation_dataset', none) is none %}
        {{ return(default__make_temp_relation(base_relation, suffix)) }}
    {% else %}
        {{ return(base_relation.incorporate(path={"schema": var('tmp_relation_dataset'),
                                        "identifier": base_relation.identifier ~ suffix})) }}
    {% endif %}
{% endmacro %}
