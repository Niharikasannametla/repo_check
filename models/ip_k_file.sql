{#- This model calls macro 'key_table_load_v_n' to generate SQL for IP keys not available in the IP_K table for all the IP relations -#}
{#- Declare dependencies to ensure dbt builds these models first -#}
{#- This helps with ordering and lineage in the DAG -#}
-- depends_on: {{ ref('employee') }}, {{ ref('department') }}
{{
    config(
        materialized='incremental', 
        unique_key='ip_sup_key'
    )
}}

{#- Get the name of the current model being compiled -#}
{%- set model_name = this.identifier -%}

{#- Retrieve the list of table names from the model's metadata (defined in YAML) -#}
{%- set models = model.meta.pm_table_names -%}
{%- do log("Models: " ~ models, info=True) -%}

{#- Retrieve the key column mappings from the model's metadata (defined in YAML) -#}
{%- set key_columns = model.meta.key_columns -%}

{#- Extract the key concept prefix from the model name (e.g., 'ip' from 'ip_k_file') -#}
{%- set parts = model_name.split("_") -%}
{%- set keyconcept = parts[0] ~ "_SUP_KEY" | upper -%}
{%- do log("keyconcept: " ~ keyconcept, info=True) -%}

{#- Resolve each model name to a dbt ref and pair it with its name -#}
{%- set resolved_models = [] -%}
{%- for m in models -%}
    {%- do resolved_models.append({'name': m, 'ref': ref(m)}) -%}
{%- endfor -%}

{#- Call the macro to generate the final SQL using the resolved models and key column metadata for IP_K table -#}
{{ key_table_load_v_n(model_name, resolved_models, key_columns) }}