{% macro lbs_to_kgs(column_name, precision=2) %}

ROUND(
    (NULLIF( {{ column_name }}, -99)/2.205)::NUMERIC, 
    {{ precision }}
)

{% endmacro %}