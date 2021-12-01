WITH fct_place_order AS (
    SELECT  
        order_id,

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

        -- Foreign Keys
        user_id,
        promo_id,
        address_id,
        tracking_id
      FROM {{ ref('stg_greenery__orders') }}
)

SELECT *
  FROM fct_place_order