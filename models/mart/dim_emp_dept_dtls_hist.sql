{% set unique_key_columns = ['EMP_ID','SYSTEM_SOURCE'] %}
{% set compare_column_list = [] %}
{% set exclude_column_list = ['EMP_ID','SYSTEM_SOURCE'] %}
{% set src_model = 'stg_emp_dept_dtls' %}
{% set sys_source = 'AUS_E' %}
{% set tgt_model = this %}
{% set condition = build_condition_column_compare(
    source_alias="s",
    target_alias="t",
    src_model_name=ref('stg_emp_dept_dtls'),
    tgt_model_name=this,
    compare_columns=compare_column_list,
    exclude_columns=exclude_column_list
) %}

{{ config(
    materialized='incremental',
    transient=false,
    post_hook=[
        update_history_records(unique_key_columns),
        apply_soft_deletes(src_model, sys_source, unique_key_columns)
    ]
) }}

with stg_dim_emp_dept_dtls as (
    select * from {{ ref('stg_dim_emp_dept_dtls') }}
),
seq_run_id as(
    select raw.seq_etl_run_id.nextval as run_id from dual
)
select
    raw.seq_dim_emp_sk.nextval as emp_sk,
    s.system_source,
    s.data_source,    
    s.deptno,
    s.department_name,
    s.location,
    s.emp_id,
    s.emp_name,
    s.manager_id,
    s.designation,
    s.total_salary,
    current_timestamp() as dw_effective_from_date,
    to_timestamp_ntz('2999-12-31 23:59:59') as dw_effective_to_date,
    'Y' as is_latest_yn,
    r.run_id,
    current_timestamp() as dw_created_date,
    current_timestamp() as dw_updated_date,    
    null as dw_deleted_flag
from stg_dim_emp_dept_dtls s join seq_run_id r
{% if is_incremental() %}
    -- Only include new or changed rows compared to the current version
    --to get the changed records
    where exists (
        select 1
        from {{ this }} t
        where t.emp_id = s.emp_id
          and t.is_latest_yn = 'Y'
          and t.dw_deleted_flag is null
          and s.system_source = t.system_source 
          and s.data_source = t.data_source
          and ( {{ condition }} )
    )
    --to get the newly inserted records
    or not exists (select 1 
                     from {{ this }} t 
                    where t.dw_deleted_flag is null 
                      and t.emp_id = s.emp_id 
                      and s.system_source = t.system_source 
                      and s.data_source = t.data_source)
{% endif %}