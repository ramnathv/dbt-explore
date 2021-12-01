
with source as (

    select * from {{ source('greenery', 'addresses') }}

),

renamed as (

    select
        address_id,

        -- Dimensions
        country,
        state,
        zipcode,
        address
    

    from source

)

select * from renamed
