{% macro bigquery__run_script_headers(script_headers) %}
    {%- if script_headers is not none -%}
        {%- if script_headers is string -%}
            {%- set script_headers = [script_headers] -%}
        {%- endif -%}

        {%- set formatted_script_headers = [] -%}
        
        {%- for script_header in script_headers -%}
            {%- set trimmed_script_header = script_header.strip() -%}
            {%- if not trimmed_script_header.endswith(';') -%}
                {%- set formatted_script_header = trimmed_script_header ~ ';' -%}
            {%- else -%}
                {%- set formatted_script_header = trimmed_script_header -%}
            {%- endif -%}
            {%- do formatted_script_headers.append(formatted_script_header) -%}
        {%- endfor -%}

        {{ formatted_script_headers | join('\n') }}
    {%- endif -%}
{%- endmacro -%}
