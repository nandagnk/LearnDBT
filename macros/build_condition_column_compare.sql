{% macro build_condition_column_compare(source_alias, target_alias, model_name, exclude_key_columns) %}
    {# model_name should be passed like ref('your_model') #}

    {% set relation = model_name %}
    {% set columns = adapter.get_columns_in_relation(relation) %}

    {% set compare_cols = [] %}

    {% for col in columns %}
        {% if col.name | lower not in exclude_key_columns %}
            {% do compare_cols.append(col.name) %}
        {% endif %}
    {% endfor %}

    {# Render the change condition #}
    {% for col in compare_cols %}
        {{ source_alias }}.{{ col }} <> {{ target_alias }}.{{ col }}
        {% if not loop.last %} or {% endif %}
    {% endfor %}
{% endmacro %}
