{{
    config(
        materialized='incremental',
        unique_key='ip_sup_key'
    )
}}

{%- set model_name = this.identifier -%}
{%- set parts = model_name.split("_") -%}
{%- set keyconcept = parts[-2] ~ "_SUP_KEY" | upper -%}
{%- do log("keyconcept: " ~ keyconcept, info=True) -%}

{%- set models = var("pm_table_name") -%}
{%- do log("Models: " ~ models, info=True) -%}
{%- set resolved_models = [] -%}
{%- for m in models -%} {%- do resolved_models.append(ref(m)) -%} {%- endfor -%}

{{- key_table_load_v1(resolved_models, model_name) -}}