
{# macros/generate_schema_name.sql #}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- set default_schema = target.schema -%}

    {# --- CI detection ---
       We check three things in order:
       1) target.name == 'ci'      -> simplest & reliable if your CI job sets its target name to 'ci'
       2) DBT_CLOUD_RUN_REASON     -> 'pull_request' for dbt Cloud PR jobs (when available)
       3) DBT_CLOUD_RUN_IS_CI      -> manual boolean flag you can set in the CI job (true/false)
    #}
    {%- set is_ci_target = (target.name | lower == 'ci') -%}
    {%- set is_pr_reason = (env_var('DBT_CLOUD_RUN_REASON') | lower == 'pull_request') -%}
    {%- set is_ci_flag   = (env_var('DBT_CLOUD_RUN_IS_CI') | lower == 'true') -%}
    {%- set is_ci = is_ci_target or is_pr_reason or is_ci_flag -%}

    {%- if is_ci -%}
        {# Use a static schema ONLY for CI runs. You can set var('ci_schema') in dbt_project.yml
           or as a Job Variable in dbt Cloud. Defaults to 'analytics_ci'. #}
        {{ var('ci_schema', 'analytics_ci') }}

    {%- elif custom_schema_name is none -%}
        {{ default_schema }}

    {%- else -%}
        {{ custom_schema_name | trim }}

    {%- endif -%}
{%- endmacro %}
