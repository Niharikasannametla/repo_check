{#- This model calls macro 'key_table_load_v1' to generate SQL for IP keys not available in the IP_K table for all the IP relations -#}
{{
    config(
        materialized='incremental',
        unique_key='ip_sup_key'
    )
}}

{#- Get the name of the current model being compiled (e.g., 'ip_k_file') -#}
{%- set model_name = this.identifier -%}

{#- Split the model name by underscores and extract the second-to-last part as the key concept -#}
{#- For example, 'ip_k_file' → parts = ['ip', 'k', 'file'] → keyconcept = 'K_SUP_KEY' -#}
{%- set parts = model_name.split("_") -%}
{%- set keyconcept = parts[-2] ~ "_SUP_KEY" | upper -%}
{%- do log("keyconcept: " ~ keyconcept, info=True) -%}

{#- Retrieve the list of source table names from the dbt variable 'pm_table_name' -#}
{%- set models = var("pm_table_name") -%}
{%- do log("Models: " ~ models, info=True) -%}

{#- Resolve each model name to a dbt ref() and collect them into a list -#}
{%- set resolved_models = [] -%}
{%- for m in models -%}
    {%- do resolved_models.append(ref(m)) -%}
{%- endfor -%}

{#- Call the macro 'key_table_load_v1' to generate the final SQL using the resolved models and current model name -#}
{{- key_table_load_v1(resolved_models, model_name) -}}