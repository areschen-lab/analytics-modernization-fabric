
SELECT
    seller_id,
    seller_zip_code_prefix,
    seller_city,
    seller_state,
    geo_city,
    geo_state,
    latitude,
    longitude
FROM {{ source('dbo', 'silver_sellers') }}
