
with source as (

    select * from {{ source('greenery', 'products') }}

),

renamed as (

    select
        product_id,

        -- Dimensions
        name AS product_name,

        -- Facts
        price AS usd_product_price,
        quantity AS nb_items_inventory

    from source

)

select * from renamed
