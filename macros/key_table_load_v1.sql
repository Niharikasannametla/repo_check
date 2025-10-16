{%- macro key_table_load_v1(relations,model_name) -%}
    {%- set re = modules.re -%}
    {%- set statements = [] -%}
    {%- set ns = namespace(counter=0) -%}
    {%- for relation in relations -%}
        {%- set keytable = model_name -%}
        {%- set parts = keytable.split('_') -%}
        {%- set keyconcept = parts[-2] | upper  -%}
        {%- set pattern = "^"~ keyconcept ~ ".*SUP_KEY$" -%}
        {%- set cols = adapter.get_columns_in_relation(relation) -%}
        {%- for col in cols -%}
            {%- set is_match = re.match(pattern, col.name, re.IGNORECASE) -%}
            {%- if is_match -%}
                {%- set columnname = col.name -%}
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
                    where k.{{ keyconcept }}_sup_key is null and v_{{ ns.counter }}.{{ keyconcept }}_sup_key is not null 
                {%- endset -%}
                {%- do statements.append(statement) -%}
                {%- set ns.counter = ns.counter + 1 -%}
            {%- endif -%}
        {%- endfor -%}
    {%- endfor -%}
    {%- if statements | length > 0 -%}
        select *
        from
            (
                {%- for stmt in statements -%}
                    {% if not loop.first %}
                        union all
                    {% endif %}
                    {{ stmt }}
                {% endfor -%}
                {{ ")" }}
    {%- endif -%}
{%- endmacro -%}