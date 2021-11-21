
with source as (

    select * from {{ source('greenery', 'order_items') }}

),

renamed as (

    select
        id,
        order_id,
        product_id,
        quantity AS item_quantity

    from source

)

select * from renamed
