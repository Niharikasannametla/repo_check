{#- This macro generates SQL query to load new IP super keys into IP_K table for all the relation tables(prep table) which have ip_sup_key -#}
{%- macro key_table_load_v1(relations, model_name) -%}

    {#- Import the regular expression module for pattern matching -#}
    {%- set re = modules.re -%}

    {#- Initialize a list to collect SQL statements for each matching column -#}
    {%- set statements = [] -%}

    {#- Create a namespace to track a counter for aliasing subqueries -#}
    {%- set ns = namespace(counter=0) -%}

    {#- Loop through each relation passed into the macro -#}
    {%- for relation in relations -%}

        {#- Extract the model name and derive the key concept from the second-to-last part of the name -#}
        {%- set keytable = model_name -%}
        {%- set parts = keytable.split("_") -%}
        {%- set keyconcept = parts[-2] | upper -%}

        {#- Build a regex pattern to match columns like IP_SUP_KEY, DEPT_SUP_KEY, etc. -#}
        {%- set pattern = "^" ~ keyconcept ~ ".*SUP_KEY$" -%}

        {#- Get all columns from the current relation (table) -#}
        {%- set cols = adapter.get_columns_in_relation(relation) -%}

        {#- Loop through each column to find matches against the pattern -#}
        {%- for col in cols -%}
            {%- set is_match = re.match(pattern, col.name, re.IGNORECASE) -%}

            {#- If the column name matches the SUP_KEY pattern, generate SQL for it -#}
            {%- if is_match -%}
                {%- set columnname = col.name -%}

                {#- Build the SQL statement for the matched column -#}
                {#- generated query checks for ip_sup_key not available in ip_k table and maps logic for other attributes of ip_k table -#}
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
                        from {{ relation }}
                    ) v_{{ ns.counter }}
                    left join {{ this }} k
                        on v_{{ ns.counter }}.{{ keyconcept }}_sup_key = k.{{ keyconcept }}_sup_key
                    where k.{{ keyconcept }}_sup_key is null
                      and v_{{ ns.counter }}.{{ keyconcept }}_sup_key is not null
                {%- endset -%}

                {#- Add the generated SQL to the statements list -#}
                {%- do statements.append(statement) -%}

                {#- Increment the counter for aliasing subqueries -#}
                {%- set ns.counter = ns.counter + 1 -%}
            {%- endif -%}
        {%- endfor -%}
    {%- endfor -%}

    {#- If any statements were generated, union them together into a final query -#}
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
    {%- endif -%}

{%- endmacro -%}
