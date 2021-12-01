with dim_address as (
    SELECT *
      FROM {{ ref('stg_greenery__addresses') }}
)

SELECT *
  FROM dim_address