{# This test directly queries the internal model created by the dbt_project_evaluator package.#}
select
    'Model names should be lowercase' as rule_description,
    resource_name as incorrect_model_value,
    directory_path
from {{ ref('int_all_graph_resources') }}
where resource_type = 'model'
  and lower(resource_name) != resource_name
