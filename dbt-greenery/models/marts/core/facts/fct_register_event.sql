WITH fct_register_event AS (
    SELECT event_id,

           -- Timestamps
           event_created_at,

           -- Foreign keys
           session_id,
           product_id,
           user_id
      FROM {{ ref('stg_greenery__events') }}
)

SELECT *
  FROM fct_register_event