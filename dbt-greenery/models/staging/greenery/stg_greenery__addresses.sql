
with source as (

    select * from {{ source('greenery', 'addresses') }}

),

renamed as (

    select
        id,
        address_id,
        address,
        zipcode,
        state,
        country

    from source

)

select * from renamed
