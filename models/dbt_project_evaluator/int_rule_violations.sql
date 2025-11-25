
select
    'Model names should be lowercase' as rule_description,
    name as offending_value
from {{ ref('int_all_graph_resources') }}
where resource_type = 'model'
