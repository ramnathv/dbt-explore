{% macro grant_usage(role) %}

    {% set sql %}
    GRANT USAGE ON SCHEMA {{ schema }} TO {{ role }};
    {% endset %}

    {% set table = run_query(sql) %}

{% endmacro %}

