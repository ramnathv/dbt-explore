WITH sessions_distinct AS (
SELECT DISTINCT session_id, checkout_at
   FROM {{ ref("fct_register_session") }}
),

metric_conversion AS (
SELECT COUNT(*) AS nb_sessions,
       SUM(CASE WHEN checkout_at IS NULL THEN 0 ELSE 1 END) AS nb_checkouts,
       SUM(CASE WHEN checkout_at IS NULL THEN 0 ELSE 1 END)::NUMERIC  / COUNT(*)AS pct_checkouts
  FROM sessions_distinct
)

SELECT *
   FROM metric_conversion