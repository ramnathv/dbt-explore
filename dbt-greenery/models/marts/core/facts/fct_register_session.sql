WITH session_product AS (
SELECT session_id,
               product_id,
               event_type,
               MAX(event_created_at) AS event_created_at_latest
   FROM {{ ref('stg_greenery__events') }} 
WHERE product_id IS NOT NULL
GROUP BY 1, 2, 3
ORDER BY session_id, event_created_at_latest
),

session_product_pivoted AS (
  SELECT session_id,
         product_id,
         {{ 
            dbt_utils.pivot(
              column='event_type',
              values=['page_view', 'add_to_cart', 'delete_from_cart'],
              then_value="event_created_at_latest",
              else_value="NULL",
              agg="MIN",
              suffix='_at'
            )
         }}
    FROM session_product
   GROUP BY 1, 2
),

session_product_pivoted_enriched AS (
SELECT *,
      CASE
         WHEN add_to_cart_at IS NULL THEN False
         WHEN delete_from_cart_at IS NULL THEN True
         WHEN add_to_cart_at > delete_from_cart_at THEN True
         ELSE False
      END AS is_in_cart
 FROM  session_product_pivoted
),

session_checkout AS (
SELECT session_id ,
       MAX(CASE WHEN event_type = 'checkout' THEN event_created_at  END) AS checkout_at
  FROM {{ ref("stg_greenery__events") }}
 GROUP  BY 1
),

session_product_enriched AS (
SELECT sc.session_id,
       spv.product_id,
       spv.page_view_at,
       spv.add_to_cart_at,
       spv.delete_from_cart_at,
       sc.checkout_at,
       spv.is_in_cart,
       spv.is_in_cart AND (checkout_at IS NOT NULL) AND (add_to_cart_at IS NOT NULL) AND (add_to_cart_at > checkout_at) AS is_in_cart_at_checkout
  FROM session_checkout AS sc
       LEFT JOIN session_product_pivoted_enriched AS spv USING(session_id)
)


SELECT *
    FROM session_product_enriched