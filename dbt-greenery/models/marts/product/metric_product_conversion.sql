WITH metric_product_conversion AS (
SELECT product_id,
       COUNT(*) AS nb_sessions,
       SUM(CASE WHEN is_in_cart THEN 1 ELSE 0 END) AS nb_cart,
       SUM(CASE WHEN is_in_cart_at_checkout THEN 1 ELSE 0 END)  AS nb_checkouts
   FROM {{ ref("fct_register_session") }}
 WHERE product_id IS NOT NULL
 GROUP BY 1
)

SELECT *, 
       nb_checkouts::NUMERIC / nb_sessions AS pct_checkouts,
       nb_cart::NUMERIC / nb_sessions AS pct_cart
  FROM metric_product_conversion