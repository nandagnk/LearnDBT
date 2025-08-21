with src_emp as
(
    select * from {{ source('raw', 'emp') }}
)
select 
    empno as emp_id,
    ename as emp_name,
    job as designation,
    hiredate as date_of_join,
    mgr as manager_id,
    sal as salary,
    comm as commission,
    nvl(sal,0)+nvl(comm,0) as total_salary,
    deptno 
from 
src_emp