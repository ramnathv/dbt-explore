WITH dim_promo AS (
    SELECT * FROM {{ ref('stg_greenery__promos') }}
)

SELECT *
  FROM dim_promo