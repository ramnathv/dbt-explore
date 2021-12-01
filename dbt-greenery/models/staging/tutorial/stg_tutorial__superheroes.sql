
with source as (

    select * from {{ source('tutorial', 'superheroes') }}

),

renamed as (

    select
        id AS superhero_id,
        name,
        gender,
        eye_color,
        race,
        hair_color,
        NULLIF(height, -99) AS height,
        publisher,
        skin_color,
        alignment,
        NULLIF(weight, -99) AS weight_lbs,
        {{ lbs_to_kgs("weight") }} AS weight_kgs,
        created_at,
        updated_at

    from source

)

select * from renamed
