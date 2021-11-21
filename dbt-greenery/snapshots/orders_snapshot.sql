{% snapshot orders_snapshot %}

{{
    config(
        unique_key='order_id',
        strategy='check',
        check_cols=['status']
    )
}}

SELECT *
  FROM {{ source('greenery', 'orders') }}

{% endsnapshot %}