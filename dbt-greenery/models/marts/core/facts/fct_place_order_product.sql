WITH fct_place_order_product AS (
    SELECT {{ dbt_utils.surrogate_key(["oi.order_id", "product_id"]) }} AS order_product_id,

          -- Facts
          nb_items,

          -- Timestamps
          o.ordered_at,
          o.estimated_delivery_at,
          o.delivered_at,

           -- Foreign Keys
           oi.order_id,
           oi.product_id,
           o.promo_id,
           o.tracking_id,
           o.user_id,
           o.address_id
           
      FROM {{ ref('stg_greenery__order_items') }} AS oi
           LEFT JOIN {{ ref('stg_greenery__orders') }} AS o ON o.order_id = oi.order_id
)

SELECT *
  FROM fct_place_order_product