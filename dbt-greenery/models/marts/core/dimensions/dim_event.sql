WITH dim_event AS (
    SELECT event_id,
           event_type,
           event_page_url,
           event_created_at
      FROM {{ ref('stg_greenery__events') }}
)

SELECT *
  FROM dim_event