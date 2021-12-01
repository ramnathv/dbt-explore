
with source as (

    select * from {{ source('greenery', 'events') }}

),

renamed as (

    select
        event_id,

        -- Dimensions
        page_url AS event_page_url,
        event_type,

        -- Timestamps
        created_at AS event_created_at,

        -- Foreign Keys
        session_id,
        user_id,
        CASE
           WHEN page_url LIKE '%product%' THEN REPLACE(page_url, 'https://greenary.com/product/', '')
        END AS product_id 

    from source

)

select * from renamed
