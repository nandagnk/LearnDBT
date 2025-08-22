{% macro handle_scd2_update() %}   
    update {{ this }} t
    set is_latest_yn = 'N',
        dw_valid_to = current_timestamp() - interval '1 minute'
    where is_latest_yn = 'Y'
    and exists (
        select 1
        from {{ this }} x
        where x.emp_id = t.emp_id
            and x.is_latest_yn = t.is_latest_yn
            and x.updated_dt > t.updated_dt
    )

{% endmacro %}
