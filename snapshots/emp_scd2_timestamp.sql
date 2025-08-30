{% snapshot emp_scd2_timestamp %}

{{
    config(        
        transient = false,
        alias = "dim_emp_dept_dtls_snapshot_1",
        target_schema = "DBT_NGOVINDAN",
        unique_key = "emp_id",
        strategy = "timestamp",
        updated_at = "updated_dt",
        invalidate_hard_deletes = True   
    )
}}
select *
from {{ ref('int_emp_dept') }}
{% endsnapshot %}