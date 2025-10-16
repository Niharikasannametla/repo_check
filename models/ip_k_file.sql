-- depends_on: {{ ref('employee') }}
-- depends_on: {{ ref('department') }}
{{
    config(
        materialized='incremental',
        unique_key='ip_sup_key'
    )
}}

{%- set model_name = this.identifier -%}
{%- set parts = model_name.split("_") -%}
{%- set keyconcept = parts[0] ~ "_SUP_KEY" | upper -%}
{%- do log("keyconcept: " ~ keyconcept, info=True) -%}

{%- set models = model.meta.pm_table_names -%}
{%- do log("Models: " ~ models, info=True) -%}
{% set resolved_models = [] %}
{% for m in models %} {% do resolved_models.append(ref(m)) %} {% endfor %}
{{- key_table_load_v_n(models, model_name) -}}