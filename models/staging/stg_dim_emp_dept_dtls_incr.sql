---implemeting SCD Type 1 using incremental strategy

---incremental_strategy 'apend'
---This is the default incremental stragy, this will always insert the records into the target, 
---this won't check whether the record already exists. This is suitable if the source is passing new records only.
---means there is no update or delete happens example : feedback or notes or logs etc.

---incremental_strategy 'merge'
---This will perform insert and update, means new records in the source will get inserted and existing records will get updated.
---This will not perform delete if any record is deleted in the source.
---This is suitable when the source is passing only the affected delta rows (like new inserts and only the updated records 
---and there won't be any delete is happening in the source)

---incremental_strategy "delete+insert"
---When the incremental strategy is append it always insert the data assume the source table is a daily full load, with some new records
---Then append will create duplicate records as everyday the source will have both new records + old records 
---This only is similar to merge only difference here is we have to manually delete the existing records

{% set incremental_strategy = var('incremental_strategy', 'merge') %}
{% set primary_key = var('unique_key', 'emp_id') %}
{% set system_source = var('sys_source', 'AUS_E') %}
{% set data_source = var('data_source', 'ORACLE') %}

{% if is_incremental() and  incremental_strategy == 'delete+insert' %}
    -- Step 1: Delete overlapping rows    
    {% set delete_statement %}
        delete from {{ this }}
        where {{ primary_key }} in (
            select {{ primary_key }}
            from {{ ref('int_emp_dept') }}
        )
        and system_source = '{{ system_source }}'
    {% endset %}    
{% endif %}


{{
    config(
        materialized='incremental',
        alias = 'stg_emp_dept_dtls_test',
        transient = false,        
        incremental_strategy = incremental_strategy,
        unique_key = primary_key,
        pre_hook = '{{delete_statement}}'
    )
}}

-- Step 2: insert 
with int_emp_dept as (
        select * from {{ ref('int_emp_dept') }}
)
select 
'{{ system_source }}' as system_source,
'{{ data_source }}' as data_source,
ied.emp_id,
ied.emp_name,
ied.manager_id,
ied.designation,
ied.total_salary,
ied.deptno,
ied.department_name,
ied.location
from int_emp_dept ied
{% if is_incremental() and incremental_strategy == 'merge' %}
    ---to get the updated rows only
    where exists (
        select 1
        from {{ this }} t
        where t.emp_id = ied.emp_id          
          and '{{ system_source }}' = t.system_source           
          and ( '{{ data_source }}' <> t.data_source or 
                ied.emp_name <> t.emp_name or 
                ied.manager_id <> t.manager_id or 
                ied.designation <> t.designation or 
                ied.total_salary <>  t.total_salary or 
                ied.department_name <> t.department_name or 
                ied.location <> t.location )
    )
    --to get the newly inserted records
    or not exists (select 1 
                     from {{ this }} t 
                    where t.emp_id = ied.emp_id 
                      and '{{ system_source }}' = t.system_source)
{% elif is_incremental() and incremental_strategy == 'append' %}    
    --to get the newly inserted records  
    where not exists (select 1 
                     from {{ this }} t 
                    where t.emp_id = ied.emp_id 
                      and '{{ system_source }}' = t.system_source)
{% endif %}