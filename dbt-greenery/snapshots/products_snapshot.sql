{% snapshot products_snapshot %}

{{
    config(
        unique_key='product_id',
        strategy='check',
        check_cols=['name', 'price', 'quantity']
    )
}}

SELECT *
  FROM {{ source('greenery', 'products') }}

{% endsnapshot %}