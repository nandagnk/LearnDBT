{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key='emp_id',
        transient=false,
        post_hook=[            
            """
            update {{ this }}
            set is_latest_yn = 'N',
                dw_valid_to = current_timestamp() - interval '1 second'
            where is_latest_yn = 'Y'
              and emp_id in (
                  select emp_id
                  from {{ this }}
                  group by emp_id
                  having count(*) > 1
              )
            """,            
            """
            update {{ this }} t
            set is_latest_yn = 'N',
                dw_valid_to = current_timestamp() - interval '1 second',
                is_deleted = 'Y'
            where is_latest_yn = 'Y'
              and is_deleted is null
              and not exists (
                  select 1 from {{ ref('src_emp') }} e where e.emp_id = t.emp_id
              )
            """
        ]
    )
}}
with int_emp_dept as (
    select * from {{ ref('int_emp_dept') }}
)
select
    s.deptno,
    s.department_name,
    s.location,
    s.emp_id,
    s.emp_name,
    s.manager_id,
    s.designation,
    s.total_salary,
    s.created_dt,
    s.updated_dt,
    'Y' as is_latest_yn,
    current_timestamp() as dw_valid_from,
    to_timestamp_ntz('2999-12-31 23:59:59') as dw_valid_to,
    null as is_deleted
from int_emp_dept s
{% if is_incremental() %}
    -- Only include new or changed rows compared to the current version
    --to get the changed records
    where exists (
        select 1
        from {{ this }} t
        where t.emp_id = s.emp_id
          and t.is_latest_yn = 'Y'
          and t.is_deleted is null
          and (
                t.updated_dt <> s.updated_dt
          )
    )
    --to get the newly inserted records
    or s.emp_id not in (select emp_id from {{ this }} where is_deleted is null)
{% endif %}