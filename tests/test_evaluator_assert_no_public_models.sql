{# This test directly queries the internal model created by the dbt_project_evaluator package.#}
select
    resource_name as model_name,
    file_path as model_path,
    access,
    CURRENT_TIMESTAMP() as load_ts,
    "Error/warn : model has public access, it should be private or protected" as comment
 
from
    {{ ref('int_all_graph_resources') }}
where
resource_type = 'model' and access='public'
