WITH mart_event AS (
    SELECT fre.*,
           de.event_type
      FROM {{ ref('fct_register_event') }} AS fre
           LEFT JOIN {{ ref('dim_event') }} AS de USING(event_id)
)

SELECT *
  FROM mart_event