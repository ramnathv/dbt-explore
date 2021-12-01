
with source as (

    select * from {{ source('greenery', 'orders') }}

),

renamed as (

    select
        order_id,

        -- Dimensions
        shipping_service,
        status AS order_status,

        -- Timestamps
        created_at AS ordered_at,
        estimated_delivery_at,
        delivered_at,

        -- Facts
        order_cost AS usd_order_cost,
        shipping_cost AS usd_shipping_cost,
        order_total AS usd_total_cost,       

        -- Foreign Keys
        user_id,
        promo_id,
        address_id,
        tracking_id
  
    from source

)

select * from renamed
