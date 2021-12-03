WITH metric_product_conversion AS (
SELECT product_id,
       COUNT(*) AS nb_sessions,
       {{ sum_if("is_in_cart") }} AS nb_cart,
       {{ sum_if("is_in_cart_at_checkout") }}  AS nb_checkouts
   FROM {{ ref("fct_register_session") }}
 WHERE product_id IS NOT NULL
 GROUP BY 1
)

SELECT *, 
       nb_checkouts::NUMERIC / nb_sessions AS pct_checkouts,
       nb_cart::NUMERIC / nb_sessions AS pct_cart
  FROM metric_product_conversion