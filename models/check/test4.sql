select
    2 as column_2,
    3 as column_3,
    4 as column_4,
    farm_fingerprint('{{ invocation_id }}') as Job_run_Id,
    {{ env_var('DBT_CLOUD_RUN_ID',0) }} as column_6