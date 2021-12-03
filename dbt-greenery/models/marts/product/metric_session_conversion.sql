WITH sessions_distinct AS (
SELECT DISTINCT session_id, checkout_at, checkout_at IS NOT NULL AS is_checkout
   FROM {{ ref("fct_register_session") }}
),

metric_conversion AS (
SELECT COUNT(*) AS nb_sessions,
       {{ sum_if("is_checkout") }} AS nb_checkouts,
       {{ sum_if("is_checkout") }}::NUMERIC  / COUNT(*) AS pct_checkouts
  FROM sessions_distinct
)

SELECT *
   FROM metric_conversion