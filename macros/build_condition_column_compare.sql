{% macro build_condition_column_compare(
    source_alias,
    target_alias,
    src_model_name,
    tgt_model_name,
    compare_columns = none,
    exclude_columns = none
) -%}

    {%- if exclude_columns is not none and exclude_columns | length > 0 -%}
        {% set exclude_upper = exclude_columns | map('upper') | list -%}		
    {%- endif -%}

    {%- if compare_columns is not none and compare_columns | length > 0 -%}	
		{% set compare_cols = compare_columns | map('upper') | list -%}        	
    {%- else -%}
        {% set src_columns = adapter.get_columns_in_relation(src_model_name) -%}
        {% set tgt_columns = adapter.get_columns_in_relation(tgt_model_name) -%}

        {% set common_columns = [] -%}
        {%- for col in src_columns -%}
            {%- for tgt_col in tgt_columns -%}
                {%- if col.name | upper == tgt_col.name | upper -%}
                    {% do common_columns.append(col.name) -%}
                {%- endif -%}
            {%- endfor -%}
        {%- endfor -%}
        
        {% set compare_cols = [] -%}
        {%- for col in common_columns -%}
            {%- if col | upper not in exclude_upper -%}
                {% do compare_cols.append(col) -%}
            {%- endif -%}
        {%- endfor -%}
    {%- endif -%}

    {# Render final condition without extra newlines #}
    {%- for col in compare_cols %}
        {{ source_alias }}.{{ col }} <> {{ target_alias }}.{{ col }}{% if not loop.last %} OR {% endif %}
    {%- endfor -%}

{%- endmacro %}