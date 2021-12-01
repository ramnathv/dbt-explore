
WITH dim_user AS (

SELECT u.user_id,
    
       -- Dimensions
       first_name,
       last_name,
       email,
       phone_number,

        -- Timestamps
        user_created_at,
        user_updated_at
  FROM {{ ref('stg_greenery__users') }} AS u
)

SELECT *
  FROM dim_user