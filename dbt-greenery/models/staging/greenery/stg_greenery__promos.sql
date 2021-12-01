
with source as (

    select * from {{ source('greenery', 'promos') }}

),

renamed as (

    select
        promo_id,

        -- Dimensions
        status AS promo_status,
        
        -- Facts
        discout AS pct_discount

    from source

)

select * from renamed
