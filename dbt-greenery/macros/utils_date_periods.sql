
{% macro utils_date_periods(start_date, end_date) %}

WITH dates AS ({{ 
    dbt_utils.date_spine(
        datepart="day",
        start_date=start_date,
        end_date=end_date
    )
}}),

-- Window Periods
windows AS (
  {% for nb_days in [7, 28, 56] %}
  (SELECT 'rolling_{{ nb_days}}d' AS period, ROW_NUMBER() OVER() AS nb_days FROM dates LIMIT {{ nb_days }})
  {% if not loop.last %} UNION {% endif %}
  {% endfor %}
),

date_periods_windows AS (
SELECT period,
       {{ dbt_utils.dateadd('day', "nb_days", "date_day") }} AS date,
       dates.date_day AS date_original
  FROM dates
       CROSS JOIN windows
),

-- Calendar Periods
calendar AS (
SELECT CASE ROW_NUMBER() OVER()  
          WHEN 1 THEN 'day' 
          WHEN 2 THEN 'week'  
          WHEN 3 THEN 'month' 
          WHEN 4 THEN 'quarter'
          WHEN 5 THEN 'year' 
       END AS period
  FROM dates
 LIMIT 5
),


date_periods_calendar AS (
SELECT period,
       DATE_TRUNC(period, "date_day") AS date,
       dates.date_day AS date_original
  FROM dates
       CROSS JOIN calendar
 WHERE period = 'week'
 ORDER BY period, date, date_original
),

-- Interval Periods
intervals AS (
  (SELECT 'Last Week' AS period, 7 as threshold FROM dates LIMIT 1) UNION
  (SELECT 'Last 2 Weeks' AS period, 7*2 as threshold FROM dates LIMIT 1)
),

date_periods_intervals AS (
SELECT period,
       CURRENT_DATE AS date,
       dates.date_day AS date_original
  FROM dates
       CROSS JOIN intervals
 WHERE dates.date_day <= CURRENT_DATE
   AND {{ dbt_utils.dateadd('day', "threshold", "date_day") }} > CURRENT_DATE
)

SELECT * FROM date_periods_windows UNION
SELECT * FROM date_periods_calendar UNION
SELECT * FROM date_periods_intervals

{% endmacro %}