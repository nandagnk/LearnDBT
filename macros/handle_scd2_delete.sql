{% macro handle_scd2_delete(src_model) %}   
    update {{ this }} t
    set is_latest_yn = 'N',
        dw_valid_to = current_timestamp() - interval '1 minute',
        is_deleted = 'Y'
    where is_latest_yn = 'Y'
    and not exists (
        select 1
        from {{ src_model }} e
        where e.emp_id = t.emp_id
    )
{% endmacro %}