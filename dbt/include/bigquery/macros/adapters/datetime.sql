{% macro generate_dates_in_range(start_date_datetime, end_date_datetime, granularity) %}
  {#-- Generate a list of datetimes between two dates #}
  {% set total_offset_seconds = (end_date_datetime - start_date_datetime).total_seconds()  %}
  {% set total_offset_hours = (total_offset_seconds/3600)|int  %}
  {% set total_offset_days = (total_offset_hours/24)|int  %}

  {% if granularity == "day" or granularity == "month"  %}
    {% set total_offset = total_offset_days  %}
  {% elif  granularity == "hour" %}
    {% set total_offset = total_offset_hours %}
  {% endif %}

  {% set date_list = [] %}
  {% for i in range(0, total_offset + 1) -%}
    {% if granularity == "day" or granularity == "month" %}
        {% set delta = modules.datetime.timedelta(days = i) %}
    {% elif granularity == "hour" %}
        {% set delta = modules.datetime.timedelta(hours = i) %}
    {% endif %}
    {% set this_date = s + delta %}
    {% set _ = date_list.append(this_date) %}
  {% endfor -%}
  {{ return(date_list) }}
{% endmacro %}
