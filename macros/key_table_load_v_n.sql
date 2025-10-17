{#- This macro generates SQL query to load new IP super keys into IP_K table for all the relation tables(prep table) which have ip_sup_key -#}
{%- macro key_table_load_v_n(model_name, relations, key_columns) -%}

    {#- Initialize an empty list to collect SQL statements -#}
    {%- set statements = [] -%}

    {#- Create a namespace to track a counter for aliasing subqueries -#}
    {%- set ns = namespace(counter=0) -%}

    {#- Extract the key concept from the model name (e.g., 'ip' from 'ip_k') -#}
    {%- set parts = model_name.split("_") -%}
    {%- set keyconcept = parts[0] | upper -%}

    {#- Loop through each relation passed in (as a dict with 'name' and 'ref') -#}
    {%- for relation in relations -%}
        {%- set cols = key_columns[relation.name] -%}

        {#- Loop through each key column defined for the relation -#}
        {%- for columnname in cols -%}

            {#- Build the SQL statement for each key column -#}
            {#- generated query checks for ip_sup_key not available in ip_k table and maps logic for other attributes of ip_k table-#}
            {%- set statement -%}
                select
                    farm_fingerprint(v_{{ ns.counter }}.{{ keyconcept }}_sup_key) as {{ keyconcept }}_id,
                    v_{{ ns.counter }}.{{ keyconcept }}_sup_key,
                    v_{{ ns.counter }}.src_stm_cd,
                    v_{{ ns.counter }}.obj_tp_cd,
                    v_{{ ns.counter }}.mstr_src_stm_cd,
                    v_{{ ns.counter }}.UNQ_ID_IN_SRC_STM,
                    1 as isrt_flow_run_id,
                    1 as isrt_job_run_id,
                    current_timestamp() as isrt_tms
                from (
                    select distinct
                        {{ columnname }} as {{ keyconcept }}_sup_key,
                        src_stm_cd,
                        split({{ columnname }}, '|')[offset(1)] as obj_tp_cd,
                        split({{ columnname }}, '|')[offset(0)] as mstr_src_stm_cd,
                        split({{ columnname }}, '|')[offset(2)] as UNQ_ID_IN_SRC_STM
                    from {{ relation.ref }}
                ) v_{{ ns.counter }}
                left join {{ this }} k
                    on v_{{ ns.counter }}.{{ keyconcept }}_sup_key = k.{{ keyconcept }}_sup_key
                where k.{{ keyconcept }}_sup_key is null
                  and v_{{ ns.counter }}.{{ keyconcept }}_sup_key is not null
            {%- endset -%}

            {#- Append the generated SQL to the statements list -#}
            {%- do statements.append(statement) -%}

            {#- Increment the counter for aliasing -#}
            {%- set ns.counter = ns.counter + 1 -%}

        {%- endfor -%}
    {%- endfor -%}

    {#- If any statements were generated, union them together -#}
    {%- if statements | length > 0 -%}
        select *
        from
            (
                {%- for stmt in statements -%}
                    {% if not loop.first %}
                        
                        union all
                    {% endif %}
                    {{ stmt }}
                {%- endfor -%}
            )
    {%- else -%}
        {#- Fallback query if no key columns or statements were found -#}
        -- No key columns found or no valid statements generated
        select null as dummy_column
    {%- endif -%}

{%- endmacro -%}
