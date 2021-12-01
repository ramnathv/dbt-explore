WITH agg_order_items_by_order AS (
    SELECT order_id,
           COUNT(DISTINCT product_id) AS nb_products_ordered,
           SUM(nb_items) AS nb_items_ordered
      FROM {{ ref('stg_greenery__order_items') }}
     GROUP BY 1
)

SELECT *
  FROM agg_order_items_by_order