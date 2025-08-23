{% macro apply_soft_deletes(src_model, unique_key_columns) %}
    {% set sql %}
        update {{ this }} t
        set is_latest_yn = 'N',
            dw_updated_date = current_timestamp(),
            dw_effective_to_date = current_timestamp() - interval '2 minute',
            dw_deleted_flag = 'Y'
        where is_latest_yn = 'Y'
          and not exists (
              select 1
              from {{ src_model }} e
              where
                  {% for col in unique_key_columns %}
                      e.{{ col }} = t.{{ col }}
                      {% if not loop.last %} and {% endif %}
                  {% endfor %}
          )
    {% endset %}

    {{ return(sql) }}
{% endmacro %}
