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
    try_to_timestamp_tz(created_at, 'YYYY-MM-DD HH24:MI:SS.FF TZHTZM')::timestamp_ntz as dept_created_dt,
    try_to_timestamp_tz(updated_at, 'YYYY-MM-DD HH24:MI:SS.FF TZHTZM')::timestamp_ntz as dept_updated_dt
from 
src_dept