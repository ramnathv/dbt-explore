WITH dim_tracking AS (
    SELECT DISTINCT tracking_id, shipping_service
      FROM {{ ref('stg_greenery__orders') }}
     WHERE tracking_id IS NOT NULL
)

SELECT *
  FROM dim_tracking