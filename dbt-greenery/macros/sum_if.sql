{% macro sum_if(column_name, value_true=1, value_false=0) %}
   SUM(CASE WHEN {{ column_name }} THEN {{ value_true }} ELSE {{ value_false }} END)
{% endmacro %}