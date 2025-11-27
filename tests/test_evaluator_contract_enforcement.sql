{# This test directly queries the internal model created by the dbt_project_evaluator package.#}
select     resource_name as model_name,
    file_path as model_path,
    is_contract_enforced as contract_enforced,
    CURRENT_TIMESTAMP() as load_ts,
    "Error/Warn: marts layer model does not have contract" as comment 
    from {{ ref('int_all_graph_resources') }} 
where is_contract_enforced != true and file_path like 'models/marts/%'
