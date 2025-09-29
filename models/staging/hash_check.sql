/*This is a sample model to generate hash value for set of column/s in a table and store it in column TA_HASH_VAL.
-The model calls the macro 'generate_table_hash_pattern'. 
-Parameter values to be passed to the macro are below:
-<1.Table name>,<2.Type of Hash function>,<3.dataset name>,<4.Optional-List of columns for hash>,<5.Optional-Columns/Pattern to be excluded>*/
SELECT 
    *,
    {{ generate_table_hash_pattern('threads_country','FARM_FINGERPRINT','ngdap_threads_core',['A.ID','B.ID','A.STATUS'],[]) }} as TA_HASH_VAL
FROM `i-t5iy-jc2qwecm-wxgvqo60sym2ze.ngdap_threads_core.threads_country` A 
left join BigQuery`i-t5iy-jc2qwecm-wxgvqo60sym2ze.ngdap_threads_core.sample_create_hook` B 
on A.ID=B.ID 