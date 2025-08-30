{{
    config(
        materialized='ephemeral'
    )
}}
with src_emp as (
    select * from {{ ref('src_emp') }}
),
src_dept as (
    select * from {{ ref('src_dept') }}
)
 select
        e.emp_id,
        e.emp_name,
        e.manager_id,
        e.designation,
        e.total_salary,
        d.deptno,
        d.department_name,
        d.location,
        e.emp_created_dt as created_dt,
        greatest(to_timestamp(e.emp_updated_dt,'DD-MON-YYYY HH24:MI:SS.FF3') , to_timestamp(d.dept_updated_dt,'DD-MON-YYYY HH24:MI:SS.FF3')) as updated_dt
    from src_emp e
    join src_dept d
        on e.deptno = d.deptno



