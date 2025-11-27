{# This test directly queries the internal model created by the dbt_project_evaluator package.#}
select
    
    resource_name as incorrect_model_name,
    file_path as model_path,
    'Error/Warn: Model names should be lowercase' as comment,
    CURRENT_TIMESTAMP() as load_ts
    
--from {{ source('evaluator_source', 'int_all_graph_resources') }}

from {{ ref('int_all_graph_resources') }}
where resource_type = 'model'
  and lower(resource_name) != resource_name

