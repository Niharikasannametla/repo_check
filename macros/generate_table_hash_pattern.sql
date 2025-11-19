{# Main macro to generate hash for multiple columns #}
{# Parameter values to be passed to the macro are below:#}
{#<1.Table name>,<2.Type of Hash function>,<3.dataset name>,<4.Optional-List of columns for hash>,<5.Optional-Columns/Pattern to be excluded>#}
{%- macro generate_table_hash_pattern(table_name, HASH_FUNC, schema_name=target.schema, columns=[], exclude_columns=[]) -%}
    {% if not columns %}
{# calling macro 'get_columns_from_table_pattern' to fetch column list from Information schema for the table details provided#}
        {% set columns = get_columns_from_table_pattern(table_name, schema_name, exclude_columns) %}
    {% endif %}
    
    {% set column_list = [] %} 
    {# Build the concatenated string for hashing #}
    {%- for column in columns -%}
        {%- do column_list.append("COALESCE(CAST(" ~ column ~ " AS STRING), CHR(0))") -%}
    {% endfor %}
    
    {{ HASH_FUNC }} (CONCAT(
        {%- for column_expr in column_list -%}
            {{ column_expr }}
            {%- if not loop.last -%},'|',{% endif %}
        {%- endfor -%}))
{% endmacro %}