
with source as (

    select * from {{ source('greenery', 'users') }}

),

renamed as (

    select
        user_id,
    
        -- Dimensions
        first_name,
        last_name,
        email,
        phone_number,

        -- Timestamps
        created_at AS user_created_at,
        updated_at AS user_updated_at,

        -- Foreign keys
        address_id

    from source

)

select * from renamed
