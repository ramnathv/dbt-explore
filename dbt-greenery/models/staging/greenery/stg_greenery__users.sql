
with source as (

    select * from {{ source('greenery', 'users') }}

),

renamed as (

    select
        id,
        user_id,
        address_id,
        first_name,
        last_name,
        email,
        phone_number,
        created_at,
        updated_at

    from source

)

select * from renamed
