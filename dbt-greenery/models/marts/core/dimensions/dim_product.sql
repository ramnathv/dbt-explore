WITH dim_product AS (
    SELECT *
      FROM {{ ref('stg_greenery__products') }}
)

SELECT *
  FROM dim_product