{# custom macro to insert model execution details into 'model_run_log' table #}
{% macro log_model_results(results) %}

{% set project_id = var('project_id') %}
{% set project_schema = var('project_schema') %}


{# Extracting model_name, model runs status, model execution time, message(rows processed information/error message in case of failure) from results object #}
    {% for result in results %}
	  {% set model_name = result.node.name %}
    {% set status = result.status %}
    {% set execution_time = result.execution_time | default(0) %}
    {% set run_id = invocation_id %}
{# escaping special characters by using replace function in the model run message to avoid error while making entry to the model run table #}
    {% set message = result.message | replace("\\", "\\\\") | replace("'", "\\'")  | replace('"', '\\"') | replace('\n', ' ') | replace('\r', ' ')  
 %}
   
    {% do run_query(
      
"INSERT INTO `" ~ project_id ~ "." ~ project_schema ~ ".model_run_log` (
     model_name, status, execution_time, message, run_timestamp, run_id
   ) VALUES (
     '" ~ model_name ~ "',
     '" ~ status ~ "',
     " ~ execution_time ~ ",
     '" ~ message ~ "',
     CURRENT_TIMESTAMP,
     '" ~ run_id ~ "'
   )"
    ) %}
  {% endfor %}
{% endmacro %}