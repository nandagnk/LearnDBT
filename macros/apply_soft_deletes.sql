{% macro apply_soft_deletes(src_model, sys_source, unique_key_columns) %}
    {% set src_relation = ref(src_model) %}
    {% set conditions %}
        {% for col in unique_key_columns %}
            s.{{ col }} = t.{{ col }}
            {% if not loop.last %} and {% endif %}
        {% endfor %}
    {% endset %}
    {% set sql %}
        update {{ this }} t
        set is_latest_yn = 'N',
            dw_updated_date = current_timestamp(),
            dw_effective_to_date = current_timestamp() - interval '2 minute',
            dw_deleted_flag = 'Y'
        where is_latest_yn = 'Y'
          and dw_deleted_flag is null
          and system_source = '{{ sys_source }}'
          and not exists (
              select 1
              from {{ src_model }} s
              where {{ conditions }}
          )
    {% endset %}
    {{ return(sql) }}
{% endmacro %}