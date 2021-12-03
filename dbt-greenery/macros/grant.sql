{% macro grant(role, select=True) %}

    {% set sql %}
      {% if select %}
      GRANT SELECT ON {{ this }} TO {{ role }};
      {% else %}
      GRANT USAGE ON SCHEMA {{ schema }} TO {{ role }};
      {% endif %}
    {% endset %}

    {% set table = run_query(sql) %}

{% endmacro %}