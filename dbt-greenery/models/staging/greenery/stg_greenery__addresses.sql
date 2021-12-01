
with source as (

    select * from {{ source('greenery', 'addresses') }}

),

renamed as (

    select
        address_id,
        address,
        -- Dimensions
        country,
        state,
        zipcode
       
    from source

)

select * from renamed
