{{
    config(
        materialized='view'
    )
}}
with src_emp as
(
    select * from {{ source('raw', 'emp') }}
)
select 
    empno as emp_id,
    upper(ename) as emp_name,
    upper(job) as designation,
    hiredate as date_of_join,
    mgr as manager_id,
    nvl(sal,0) as salary,
    nvl(comm,0) as commission,
    nvl(sal,0)+nvl(comm,0) as total_salary,
    deptno,
    created_at as emp_created_dt,
    updated_at as emp_updated_dt
from 
src_emp