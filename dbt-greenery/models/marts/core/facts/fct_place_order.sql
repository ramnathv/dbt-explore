WITH fct_place_order AS (
    SELECT  
        o.order_id,

        -- Timestamps
        ordered_at,
        estimated_delivery_at,
        delivered_at,

        -- Facts
        usd_order_cost,
        usd_shipping_cost,
        usd_total_cost,      

        -- Computed
        {{ dbt_utils.datediff("ordered_at", "delivered_at", 'day') }} AS nb_days_delivery, 
        {{ dbt_utils.datediff("estimated_delivery_at", "delivered_at", 'day') }} AS nb_days_delay_delivery, 

        -- Aggregates
        oio.nb_items_ordered,
        oio.nb_products_ordered,

        -- Foreign Keys
        user_id,
        promo_id,
        address_id,
        tracking_id
      FROM {{ ref('stg_greenery__orders') }} AS o
           LEFT JOIN {{ ref('agg_order_items_by_order') }} AS oio ON oio.order_id = o.order_id
)

SELECT *
  FROM fct_place_order