select
    *,
    '123|aaa|bbb' as ip_sup_key,
    '12211213|wewe|bewwewebb'  as ip_asd_ip_sup_key,
    '1211s3|aasda|dbbb' as cust_cl_sup_key,
    'test' src_stm_cd
from

    (
        select
            1 as employee_id,
            'Alex' as employee_name,
            100 as emp_department_id,
            'London' as employee_location,
            1000 as employee_salary

        union all

        select
            2 as employee_id,
            'George' as employee_name,
            100 as emp_department_id,
            'Amsterdam' as employee_location,
            2000 as employee_salary

        union all

        select
            3 as employee_id,
            'David' as employee_name,
            200 as emp_department_id,
            'Stockholm' as employee_location,
            2500 as employee_salary

        union all

        select
            4 as employee_id,
            'Martin' as employee_name,
            300 as emp_department_id,
            'London' as employee_location,
            2000 as employee_salary

        union all

        select
            5 as employee_id,
            'Robert' as employee_name,
            400 as emp_department_id,
            'Paris' as employee_location,
            5000 as employee_salary
    )