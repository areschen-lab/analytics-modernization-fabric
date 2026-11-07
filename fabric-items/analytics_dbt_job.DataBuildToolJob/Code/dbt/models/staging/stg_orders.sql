SELECT
    *
FROM {{ source('dbo', 'silver_orders') }}
