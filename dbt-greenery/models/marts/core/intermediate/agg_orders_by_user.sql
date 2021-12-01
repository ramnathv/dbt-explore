/*
## Orders Aggregated by User

```dbt
columns:
	- name: nb_orders
		description: >
			The total number of orders placed by the user
  - name: usd_purchase_cost
		description: >
			The total amount spent on purchase.
		meta:
			title: Purchase Cost
	- name: usd_shipping_cost
		description: >
			The total amount spent on shipping.
		meta:
			title: Shipping Cost
	- name: usd_total_cost
		description: >
			The total amount spent on the order.
	- name: usd_avg_cost
	  description: >
	  		The average amount spent on an order

```
*/
WITH agg_orders_by_user AS (
SELECT user_id,
       COUNT(DISTINCT order_id) AS nb_orders,
       SUM(usd_order_cost) AS usd_order_cost,
       SUM(usd_shipping_cost) AS usd_shipping_cost,
       SUM(usd_total_cost) AS usd_total_cost,
       SUM(usd_total_cost) / COUNT(DISTINCT order_id) AS usd_avg_cost
  FROM {{ ref('stg_greenery__orders') }}
 GROUP BY 1
)

SELECT *
  FROM agg_orders_by_user
