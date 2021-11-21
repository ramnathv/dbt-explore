
with source as (

    select * from {{ source('greenery', 'promos') }}

),

renamed as (

    select
        id,
        promo_id,
        discout AS discount,
        status AS promo_status

    from source

)

select * from renamed
