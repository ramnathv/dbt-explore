WITH session_events AS (
 SELECT session_id,
        event_type AS event_from,
        ROW_NUMBER() OVER(PARTITION BY session_id ORDER BY event_created_at) AS event_step,
        LEAD(event_type) OVER(PARTITION BY session_id ORDER BY event_created_at) AS event_to
   FROM {{ ref("mart_event") }} 
),

session_events_enriched AS (
SELECT *,
       event_step || '_' || event_from AS event_from_with_step,
       event_step || '_' || event_to AS event_to_with_step
  FROM session_events
)

SELECT event_from,
       event_to,
       event_from_with_step,
       event_to_with_step,
       COUNT(*) AS nb_sessions
  FROM session_events_enriched
 GROUP BY 1, 2, 3, 4