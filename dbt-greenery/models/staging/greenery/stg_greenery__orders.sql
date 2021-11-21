
with source as (

    select * from {{ source('greenery', 'orders') }}

),

renamed as (

    select
        id,
        order_id,
        user_id,
        promo_id,
        address_id,
        tracking_id,

        created_at,
        order_cost,
        shipping_cost,
        order_total,

        shipping_service,
        status AS order_status,

        estimated_delivery_at,
        delivered_at
  

    from source

)

select * from renamed
