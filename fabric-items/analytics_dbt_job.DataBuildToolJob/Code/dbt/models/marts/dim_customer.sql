
SELECT
    customer_id,
    customer_unique_id,
    customer_zip_code_prefix,
    customer_city,
    customer_state,
    geo_city,
    geo_state,
    latitude,
    longitude
FROM {{ source('dbo', 'silver_customers') }}