
with source as (

    select * from {{ source('greenery', 'order_items') }}

),

renamed as (

    select
        id AS order_item_id,
     
        -- Measures
        quantity AS nb_items,

        -- Foreign keys
        order_id,
        product_id

    from source

)

select * from renamed
