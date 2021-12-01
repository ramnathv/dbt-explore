WITH agg_events_by_user AS (
    SELECT user_id,
           COUNT(DISTINCT event_id) AS nb_events,
           COUNT(DISTINCT session_id) AS nb_sessions
      FROM {{ ref('stg_greenery__events') }}
     GROUP BY 1
)

SELECT *
  FROM agg_events_by_user