/*--"unique_key_columns" is to configure the primary/unique key to identify the rows*/
{% set unique_key_columns = ['EMP_ID','SYSTEM_SOURCE','DATA_SOURCE'] %}

/*
---"compare_column_list" is to configure the columns to be compared between the source and target 
---to determine the updated rows(when there is a difference). This is optional and when no values 
---passed macro will derive the common columns between the source and target.
*/

{% set compare_column_list = [] %}

/*
---"exclude_column_list" is to configure the columns can be ignored in the comparison
---it is good to exclude the unique_key_columns in the exclude column list since these columns won't 
---vary between source and target.
*/

{% set exclude_column_list = ['EMP_ID','SYSTEM_SOURCE','DATA_SOURCE'] %}


/*
---configure the dynamic data comparison string to check any data is changed between stg and dim
---This is required to indentify the updated records for incremental load
---Note:- make sure to pass values for either "compare_column_list" or "exclude_column_list"
*/

{% set condition = build_condition_column_compare(source_alias="s", target_alias="t", src_model_name=ref('stg_dim_arrears_gnk'), tgt_model_name =  this ,  compare_columns=compare_column_list, exclude_columns=exclude_column_list) %}

{{
    config(
        materialized='incremental',               
        transient=false,
        post_hook=[ 
            update_history_records(unique_key_columns),
            apply_soft_deletes(ref('stg_dim_emp_dept_dtls'), unique_key_columns)            
        ]
    )
}}
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