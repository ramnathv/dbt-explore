WITH dim_order AS (
    SELECT order_id,
           order_status,
           ordered_at,
           estimated_delivery_at,
           delivered_at
      FROM {{ ref('stg_greenery__orders') }}
)

SELECT *
  FROM dim_order