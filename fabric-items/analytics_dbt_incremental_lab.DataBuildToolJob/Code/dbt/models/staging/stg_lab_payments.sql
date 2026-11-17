{{ config(materialized='view') }}

SELECT p.*
FROM {{ source('dbo', 'silver_order_payments') }} p

INNER JOIN {{ ref('stg_lab_orders') }} o
    ON p.order_id = o.order_id