{% snapshot superheroes_snapshot %}
{{
    config(
        unique_key='id',
        strategy='timestamp',
        updated_at='updated_at'
    )
}}

SELECT *
  FROM {{ source('greenery', 'superheroes') }}


{% endsnapshot %}