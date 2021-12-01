WITH metric_event AS (
    SELECT dp.date,
           event_type,
           COUNT(DISTINCT user_id) AS nb_users
      FROM {{ ref('mart_event') }} AS me
           LEFT JOIN {{ ref('date_periods') }} AS dp ON dp.date_original = DATE(me.event_created_at)
     WHERE dp.period = 'rolling_28d'
     GROUP BY 1, 2
),

metric_event_pivoted AS (
SELECT date,
       {{ dbt_utils.pivot(
            column = "event_type", 
            values = dbt_utils.get_column_values(
              ref("mart_event"),
              'event_type'
            ),
            then_value = "nb_users"
          )
       }}
  FROM metric_event
 GROUP BY 1
)

SELECT *
  FROM metric_event_pivoted