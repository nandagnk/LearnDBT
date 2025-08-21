{{
    config(
        materialized='incremental',
        transient = false,
        unique_key = 'emp_id',
        incremental_strategy='merge',
        post_hook = "
            delete from {{ this }}
            where not exists (
                select 1
                from {{ ref('src_emp') }} as e
                where e.emp_id = {{ this }}.emp_id
            )
        "
    )
}}
with src_emp as
(
    select * from {{ ref('src_emp') }}
),
src_dept as
(
    select * from {{ ref('src_dept') }}
)
select 
    d.deptno,
    d.department_name,
    d.location,
    e.emp_id,
    e.emp_name,
    e.manager_id,
    e.designation,
    e.total_salary
from src_emp e join src_dept d on (e.deptno = d.deptno)