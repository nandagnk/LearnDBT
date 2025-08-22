{{
    config(
        materialized='incremental',
        transient = false,
        pre_hook = [
             "delete from {{ this }} where system_source = 'AUS_E'"
        ]
    )
}}

with int_emp_dept as (
        select * from {{ ref('int_emp_dept') }}
)
select 
'AUS_E' as system_source,
'ORACLE' as data_source,
ied.emp_id,
ied.emp_name,
ied.manager_id,
ied.designation,
ied.total_salary,
ied.deptno,
ied.department_name,
ied.location
from int_emp_dept ied