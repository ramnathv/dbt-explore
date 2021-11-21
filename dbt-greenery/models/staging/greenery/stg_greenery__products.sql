
with source as (

    select * from {{ source('greenery', 'products') }}

),

renamed as (

    select
        id,
        product_id,
        name,
        price,
        quantity AS product_quantity

    from source

)

select * from renamed
