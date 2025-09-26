{# Macro to get columns from BigQuery table using the table and schema details provided #}
{# column list is fetched from INFORMATION_SCHEMA.COLUMNS #}
{# columns not required as part of hash calculation are exluded #}
{% macro get_columns_from_table_pattern(table_name, schema_name, exclude_columns_pattern=[]) %}
    {% set table_query %}
        SELECT column_name
        FROM {{ schema_name }}.INFORMATION_SCHEMA.COLUMNS
        WHERE table_name = '{{ table_name }}' 
            {%- for col in exclude_columns_pattern -%}
              AND column_name NOT like '%{{ col }}%'  
                {%- if not loop.last -%}
                {% endif %}
            {%- endfor -%}
        ORDER BY ordinal_position
    {% endset %}
    
    {% set results = run_query(table_query) %}
    {% set columns = [] %}
    
    {% if execute %}
        {% for row in results %}
            {% do columns.append(row[0]) %}
        {% endfor %}
    {% endif %}
    
    {{ return(columns) }}
{% endmacro %}