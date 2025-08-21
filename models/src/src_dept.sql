with src_dept as
(
    select * from {{ source('raw', 'dept') }}
)
select 
    deptno,
    dname as DEPARTMENT_NAME,
    loc   as location
from 
src_dept