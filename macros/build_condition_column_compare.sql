{% macro build_condition_column_compare(source_alias, target_alias, model_name) %}
    {# model_name should be passed like ref('your_model') #}

    {% set relation = model_name %}
    {% set columns = adapter.get_columns_in_relation(relation) %}

    {# Exclude key columns #}
    {% set key_cols = ['emp_id', 'system_source', 'data_source'] %}
    {% set compare_cols = [] %}

    {% for col in columns %}
        {% if col.name | lower not in key_cols %}
            {% do compare_cols.append(col.name) %}
        {% endif %}
    {% endfor %}

    {# Render the change condition #}
    {% for col in compare_cols %}
        {{ source_alias }}.{{ col }} <> {{ target_alias }}.{{ col }}
        {% if not loop.last %} or {% endif %}
    {% endfor %}
{% endmacro %}
