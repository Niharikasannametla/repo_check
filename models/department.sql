 select
    *,
    '12345|hello|basabb' as ip_sup_key,
    '121188|ok|bbb' as ip_cust_sup_key,
    '444|jtrjr|44bbb' as cl_domain_sup_key,
    'testnew_dept' src_stm_cd
from
    (

        select
            100 as department_id,
            'HR' as department_name,
            'London' as department_location

        union all

        select
            200 as department_id,
            'IT admin' as department_name,
            'Stockholm' as department_location

        union all

        select
            300 as department_id,
            'Sales' as department_name,
            'Amsterdam' as department_location

        unionall

        select
            400 as department_id,
            'Cloud' as department_name,
            'Paris' as department_location

    )