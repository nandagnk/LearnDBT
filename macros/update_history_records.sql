{% macro update_history_records(unique_key_columns) %}
    {% set sql %}
        update {{ this }} t
        set is_latest_yn = 'N',
            dw_updated_date = current_timestamp(),
            dw_effective_to_date = current_timestamp() - interval '2 minute'
        where is_latest_yn = 'Y'
          and dw_deleted_flag is null
          and exists (
              select 1
              from {{ this }} x
              where
                  {% for col in unique_key_columns %}
                      x.{{ col }} = t.{{ col }}
                      {% if not loop.last %} and {% endif %}
                  {% endfor %}
                  and x.is_latest_yn = t.is_latest_yn
                  and x.dw_created_date > t.dw_created_date
                  and x.dw_deleted_flag is null
          )
    {% endset %}

    {{ return(sql) }}
{% endmacro %}