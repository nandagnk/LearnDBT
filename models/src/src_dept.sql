{{
    config(
        materialized='view'
    )
}}
with src_dept as
(
    select * from {{ source('raw', 'dept') }}
)
select 
    deptno,
    upper(dname) as DEPARTMENT_NAME,
    upper(loc)   as location,
    created_at as dept_created_dt,
    updated_at as dept_updated_dt
from 
src_dept