{% snapshot users_snapshot %}

{{
    config(
        unique_key='user_id',
        strategy='timestamp',
        updated_at='updated_at'
    )
}}

SELECT *
  FROM {{ source('greenery', 'users') }}

{% endsnapshot %}