
WITH fct_user AS (

SELECT u.user_id,
        -- Facts: Aggregated from orders
        aou.nb_orders,
        aou.usd_order_cost,
        aou.usd_shipping_cost,
        aou.usd_avg_cost,

        -- Facts: Aggregated from events
        aeu.nb_events,
        aeu.nb_sessions

  FROM {{ ref('stg_greenery__users') }} AS u
       LEFT JOIN {{ ref('agg_orders_by_user') }} AS aou USING(user_id)
       LEFT JOIN {{ ref('agg_events_by_user') }} AS aeu USING(user_id)

)

SELECT *
  FROM fct_user