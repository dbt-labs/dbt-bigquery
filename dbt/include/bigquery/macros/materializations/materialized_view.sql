{% materialization materialized_view, adapter='bigquery' %}
    -- this is a copy/paste of the view materialization

    -- grab current tables grants config for comparison later on
    {% set grant_config = config.get('grants') %}
    {% set to_return = create_or_replace_view() %}
    {% set target_relation = this.incorporate(type='view') %}

    {% do persist_docs(target_relation, model) %}

    {% if config.get('grant_access_to') %}
        {% for grant_target_dict in config.get('grant_access_to') %}
            {% do adapter.grant_access_to(this, 'view', None, grant_target_dict) %}
        {% endfor %}
    {% endif %}

    {% do return(to_return) %}

{% endmaterialization %}
